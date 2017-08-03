#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <math.h>
#include <string.h>
#include <time.h>

#define RECORD_SIZE 64
#define KEY_LENGTH 8

void print_array(char* start, size_t len);
void* workerFunc(void* args);
int cmpfunc (const void * a, const void * b);
int test_ordered(char* start, size_t len);
int mergeFunction(char* block_addr_1, size_t block_length_1, char* block_addr_2, size_t block_length_2);

struct mem_block
{
    int thread_id;
    int slave_tid;
    int t_level;
    char* start_addr;
    int wait_skip;
    int wait_skip_limit;
    size_t length;
};

unsigned short int total_num_threads;
struct mem_block *blocks;
pthread_t *threads;

pthread_cond_t suspend_cond;
pthread_mutex_t suspend_mutex;

int init_flag = 0, verbose_flag = 0, test_flag = 0;
int num_runs = 1;
float total_tight_time = 0.0;
int main(int argc, char** argv)
{
    FILE* file;
    char* map_addr;
    int file_desc;
    struct stat file_stats;
    size_t file_len, szper;
    int num_proc_onln, num_threads;
    int thread_override = -1;
    int pages_per_block, page_sz, num_pages;
    int data_sz;


    // Start by retrieving and checking the arguments.
    if(argc <= 2)
    {
        if(argc == 2)
        {
            if(strncmp(argv[1], "-", 1) == 0)
            {
                fprintf(stderr, "Usage: %s -[v, t <num_runs>, n <num_threads>] <filename>\n", argv[0]);
            }
        }
        else
        {
            fprintf(stderr, "Usage: %s -[v, t <num_runs>, n <num_threads>] <filename>\n", argv[0]);
            exit(1);
        }
    }
    else
    {
        int i;
        for(i = 1; i < argc-1; i++)
        {
            if(strcmp(argv[i], "-v") == 0)
            {
                //Engage verbose mode.
                verbose_flag = 1;
            }
            else if (strcmp(argv[i], "-n") == 0)
            {
                thread_override = atoi(argv[i+1]);
            }
            else if (strcmp(argv[i], "-t") == 0)
            {
                test_flag = 1;
                num_runs = atoi(argv[i+1]);
            }
        }
    }
    // Open our file and make sure it actually exists.
    if((file_desc = open(argv[argc - 1], O_RDWR)) == -1)
    {
        fprintf(stderr, "There was an issue opening the file.\n");
        exit(2);
    }
    if(fstat(file_desc, &file_stats) == -1)
    {
        fprintf(stderr, "There was an issue reading the file's size.\n");
        exit(3);
    }
    // Check to make sure the file is actually mappable
    if(file_stats.st_size == 0)
    {
        fprintf(stderr, "The file must have at least some data in it...\n");
        exit(4);
    }

    int run_index;
    float run_time_avg = 0;
    for(run_index = 0; run_index < num_runs; run_index++)
    {
        
        
        // Map file to memory.
        if((map_addr = mmap(NULL, file_stats.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, file_desc, 0)) == NULL)
        {
            fprintf(stderr, "There was a problem mapping the file. \n");
            exit(5);
        }

        
        num_proc_onln = sysconf(_SC_NPROCESSORS_ONLN); // Get the system to tell us how many processors we have.
        page_sz = sysconf(_SC_PAGE_SIZE); // Get the system's page size.
        num_pages = (int)ceilf(((float)file_stats.st_size)/page_sz); // Give an upper bound on the number of pages we need.
        if(num_pages < num_proc_onln)
        {
            num_threads = num_pages;
            if(thread_override != -1)
            {
                fprintf(stderr, "\n*** WARNING: Cannot use thread override - too large to sustain page alignment. ***\n*** Using %d threads instead. ***\n\n", num_threads);
            }
        }
        else
        {
            num_threads = num_proc_onln;
            if(thread_override != -1)
            {
                float b = (float)(log(thread_override)/log(2.0));
                if((b - floor(b)) != 0.0)
                {
                    fprintf(stderr, "'-n' argument should be a power of 2\n");
                    exit(10);
                }
                
                num_threads = thread_override;
            }
        }
        
        // Build our file blocks.
        blocks = calloc(num_threads, sizeof(struct mem_block));
        if(blocks == NULL)
        {
            fprintf(stderr, "Issue allocating memory for thread blocks.\n");
            exit(6);
        }

        // Build our array for threading.
        threads = calloc(num_threads, sizeof(pthread_t));
        if(threads == NULL)
        {
            fprintf(stderr, "There was an issue building the TID array.\n");
            exit(7);
        }

        szper = (size_t) ceil(file_stats.st_size / num_threads); // Calculate the number of bytes per thread block.
        pages_per_block = (int) ceilf((((float)file_stats.st_size) / num_threads + file_stats.st_size % num_threads) / page_sz); // Page align that.
        
        if(verbose_flag == 1)
        {
            // ========================== Print out info ===============================
            printf("==================================================================\n");
            printf("This system has (%d) processors for use\n", num_proc_onln);
            printf("This system's page size is: %d\n", page_sz);
            printf("The input file's size is: %lld\n", file_stats.st_size);
            printf("We are able to utilize %d threads\n", num_threads);
            printf("Pages Per Block: %d\n", pages_per_block);
            printf("Maximum # of pages: %d\n", num_pages);
            printf("Map Starting Address: %lx\n", (unsigned long) map_addr);
            
            printf("Map First Record: \" ");
            int ind;
            // Subtracting 2 from RECORD_SIZE to get rid of CL and RF characters.
            for(ind = 0; ind < RECORD_SIZE-2; ind++)
            {
                printf("%c", map_addr[ind]);
            }
            printf(" \"\n");
            printf("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n\n");
        }
        
        // ===========================================================================================================================================================
            
        
        unsigned short int block_ind;
        char* current_addr = map_addr;
        int data_count = 0;
        total_num_threads = num_threads;
        if(verbose_flag == 1)
        {
            printf("Starting Transfer Block Creation...\n");
            printf("==================================================================\n");
        }
        for(block_ind = 0; block_ind < num_threads; block_ind++)
        {
            //Last block is special because we want to make sure we have all our data.
            if(block_ind == num_threads - 1)
            {
                blocks[block_ind].start_addr = current_addr;
                blocks[block_ind].length = (size_t) (file_stats.st_size - data_count);
            }
            else
            {
                blocks[block_ind].start_addr = current_addr;
                blocks[block_ind].length = (pages_per_block * page_sz);
            }
            blocks[block_ind].thread_id = block_ind;
            
            if(block_ind % 2 == 0)
            {
                blocks[block_ind].wait_skip = 1;
                blocks[block_ind].wait_skip_limit = (block_ind == 0 || block_ind == total_num_threads/2) ? total_num_threads/2 : (int)(log(total_num_threads)/log(2.0)) - 1;
            }
            else
            {
                blocks[block_ind].wait_skip = -1;
            }
            current_addr += blocks[block_ind].length;
            data_count += szper;
            blocks[block_ind].t_level = -1;
            if(verbose_flag == 1)
            {
                printf("%d Block[ %d ] | Start Address: %lx | Length: %zu\n", blocks[block_ind].wait_skip == -1, block_ind, (unsigned long)blocks[block_ind].start_addr, blocks[block_ind].length);
            }
        }
        
        if(verbose_flag == 1)
        {
            printf("\nTotal File Size: %lld, Total Data Encapsulated: %d\n", file_stats.st_size, data_count);
            printf("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n\n");
        }
        
        pthread_mutex_init(&suspend_mutex, NULL);
        pthread_cond_init(&suspend_cond, NULL);
        unsigned short int threads_to_create = num_threads;
        int skip = 1; // This is the size of the jump iterating over the thread id's.
        
        if(verbose_flag == 1)
        {
            printf("Starting MergeSort...\n");
            printf("==================================================================\n");
        }
        total_tight_time = 0.0;
        struct timespec time_start, time_end;
        
        int level = 0;
        
        if(verbose_flag == 1)
        {
            printf("Number Of Threads To Be Created: %d\n", threads_to_create);
        }
        
        //Set our wait condition for the threads.
        pthread_mutex_lock(&suspend_mutex);
        init_flag = 0;
        pthread_cond_broadcast(&suspend_cond);
        pthread_mutex_unlock(&suspend_mutex);

        // Create threads according to skip
        if(verbose_flag)
        {
            printf("Creating Thread: ");
        }
        for(block_ind = 0; block_ind < num_threads; block_ind++)
        {
            if(verbose_flag)
            {
                printf("%d.. ", block_ind);
            }
            if(pthread_create(&threads[block_ind], NULL, workerFunc, (void*)&blocks[block_ind]) != 0)
            {
                fprintf(stderr, "\nThere was a problem creating a thread.\n");
                exit(8);
            }
        }
        if(verbose_flag)
        {
            printf("\n");
        }
        
        // Once they've been created - let 'em rip!
        pthread_mutex_lock(&suspend_mutex);
        init_flag = 1;
        clock_gettime(CLOCK_REALTIME, &time_start);
        pthread_cond_broadcast(&suspend_cond);
        pthread_mutex_unlock(&suspend_mutex);
        
        //Wait for 0th thread.
        pthread_join(threads[0], NULL);
        
        clock_gettime(CLOCK_REALTIME, &time_end);
        float total_time = (time_end.tv_sec + ((float)time_end.tv_nsec / 1000000000.0)) - (time_start.tv_sec + ((float)time_start.tv_nsec / 1000000000.0));
        if(test_flag)
        {
            run_time_avg += total_time;
        }
        if(verbose_flag)
        {
            printf("Total Time Taken: %f sec.\n", total_time);
            
            if(test_ordered(map_addr, file_stats.st_size) == 1)
            {
                printf("The array is verified to be in order!\n");
            }
            else
            {
                printf("WARNING: The final array IS NOT in order.\n");
            }
        }
        
        //printf("Printing Final, Sorted Array Keys: \n");
        //print_array(map_addr, file_stats.st_size);
        //printf("\n");
        
        // Close all of our resources
        
        pthread_cond_destroy(&suspend_cond);
        pthread_mutex_destroy(&suspend_mutex);
        free(threads);
        free(blocks);
        munmap(map_addr, file_stats.st_size);
    }
    run_time_avg /= num_runs;
    total_tight_time /= num_runs;
    if(test_flag == 1)
    {
        printf("Avg run-time for %d threads over %d runs: %f | (tight): %f\n", total_num_threads, num_runs, run_time_avg, total_tight_time);
    }
    fclose(file);
    
    return 0;
}

int cmpfunc (const void * a, const void * b)
{
    const char * pa = (const char *) a;
    const char * pb = (const char *) b;
    return strncmp(pa, pb, 8);
}
void print_array(char* start, size_t len)
{
    printf("[ ");
    char *i = 0;
    char  j = 0;
    for(i = start; i < (start + len); i+=RECORD_SIZE)
    {
        for(j = 0; j < KEY_LENGTH; j++)
        {
            printf("%c", *(i+j));
        }
        printf(", ");
    }
    printf(" ]\n");
}
int test_ordered(char* start, size_t len)
{
    unsigned int num_elems = 0;
    unsigned int num_ordered = 0;
    char* i;
    int result = 0;
    int last_result = 0;
    char k1[8], k2[8];

    for(i = start; i < ((start + len) - RECORD_SIZE); i += RECORD_SIZE)
    {
        strncpy(k1, i, 8);
        strncpy(k2, i+RECORD_SIZE, 8);
        result = strncmp(i, i + RECORD_SIZE, 8);
        
        if ((last_result < 0 || last_result == 0) && result > 0)
        {
            printf("ERROR: ");
            printf("Result is: %d, but was: %d\n", result, last_result);
        }
        if(result < 0 || result == 0)
        {
            num_ordered++;
        }
        last_result = result;
        num_elems++;
    }
    if(num_ordered == num_elems)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}
void* workerFunc(void* args)
{
    // Wait for all the other threads to be created before we begin.
    pthread_mutex_lock(&suspend_mutex);
    while(init_flag == 0)
    {
        pthread_cond_wait(&suspend_cond, &suspend_mutex);
    }
    pthread_mutex_unlock(&suspend_mutex);
    
    // Cast the pointer for the data encapsulator struct, so we have our parameters.
    struct mem_block *work_pack = (struct mem_block*) args;
    struct timespec time_start, time_finish;
    float qsort_time, merge_time;
    
// Sort Call -------->
    clock_gettime(CLOCK_REALTIME, &time_start);
    qsort(work_pack->start_addr, work_pack->length/RECORD_SIZE, RECORD_SIZE, cmpfunc);
    clock_gettime(CLOCK_REALTIME, &time_finish);
// ------------------>
    
    qsort_time = (time_finish.tv_sec + ((float)time_finish.tv_nsec / 1000000000.0)) - (time_start.tv_sec + ((float)time_start.tv_nsec / 1000000000.0));
    total_tight_time += qsort_time;
    if(verbose_flag == 1)
    {
        printf("Thread [ %d ]: QSort took %f sec to complete.\n", work_pack->thread_id, qsort_time);
    }
    
    if(work_pack->wait_skip != -1)
    {
        //0 needs to wait for 1, 2, 4
        //1 is a leaf
        //2 needs to wait for 3
        //3 is a leaf
        //4 needs to wait for 5, 6
        //5 is a leaf
        //6 needs to wait for 7
        //7 is a leaf
        int wait_for = 0;
        while(work_pack->wait_skip <= work_pack->wait_skip_limit && work_pack->thread_id + work_pack->wait_skip < total_num_threads)
        {
            wait_for = work_pack->thread_id + work_pack->wait_skip;
            
            if(verbose_flag == 1)
            {
                printf("Thread [ %d ]: Joining with [ %d ] | Current Skip: %d <= Current WSL: %d\n", work_pack->thread_id, wait_for, work_pack->wait_skip, work_pack->wait_skip_limit);
            }
            
            pthread_join(threads[wait_for], NULL);
// MERGE CALL ------>
            clock_gettime(CLOCK_REALTIME, &time_start);
            mergeFunction(work_pack->start_addr, work_pack->length, blocks[wait_for].start_addr, blocks[wait_for].length);
            clock_gettime(CLOCK_REALTIME, &time_finish);
// ----------------->
            
            merge_time = (time_finish.tv_sec + ((float)time_finish.tv_nsec / 1000000000.0)) - (time_start.tv_sec + ((float)time_start.tv_nsec / 1000000000.0));
            total_tight_time += merge_time;
            if(verbose_flag == 1)
            {
                printf("Thread [ %d ]: Merge took %f sec to complete.\n", work_pack->thread_id, merge_time);
            }
            
            blocks[work_pack->thread_id].length += blocks[wait_for].length;
            work_pack->wait_skip *= 2;
        }
    }
    if(verbose_flag == 1)
    {
        printf("Thread [ %d ] EXITING!\n", work_pack->thread_id);
    }
    pthread_exit(0);
}


int mergeFunction(char* block_addr_1, size_t block_length_1, char* block_addr_2, size_t block_length_2)
{
    //Calculate the final size of the merge block.
    size_t final_size = block_length_1 + block_length_2;
    unsigned long b1_ind = 0, b2_ind = 0, merge_ind = 0;
    unsigned long n_b1 = block_length_1/RECORD_SIZE, n_b2 = block_length_2/RECORD_SIZE;
    unsigned long data_copied = 0;
    char* block_1_copy;
    char* block_2_copy;
    char* merge_addr;
    merge_addr = block_addr_1;
    //printf("Merge Block Size: %ld\n", final_size);
    if((block_1_copy = (char*) mmap(NULL, block_length_1, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0)) == NULL)
    {
        return -1;
    }
    // copy block 1 to alternate location
    memcpy((char*) block_1_copy, (char*) block_addr_1, block_length_1);
    if((block_2_copy = (char*) mmap(NULL, block_length_2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0)) == NULL)
    {
        return -1;
    }
    // copy block 2 to alternate location
    memcpy((char*) block_2_copy, (char*) block_addr_2, block_length_2);
    
    while(b1_ind < n_b1 || b2_ind < n_b2)
    {
        if(b1_ind == n_b1)
        {
            // Complete transfer for B1.
            memcpy((char*) (merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_2_copy + (b2_ind++ * RECORD_SIZE)), RECORD_SIZE);
        }
        else if(b2_ind == n_b2)
        {
            //Complete transfer for B2.
            memcpy((char*) (merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_1_copy + (b1_ind++ * RECORD_SIZE)), RECORD_SIZE);
        }
        else
        {
            int cmp_res = strncmp((char*) (block_1_copy + (b1_ind * RECORD_SIZE)), (char*) (block_2_copy + (b2_ind * RECORD_SIZE)), KEY_LENGTH);
            if(cmp_res == 0)
            {
                //Transfer element from both B1 & B2 in-place.
                memcpy((char*)(merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_1_copy + (b1_ind++ * RECORD_SIZE)), RECORD_SIZE);
                memcpy((char*)(merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_2_copy + (b2_ind++ * RECORD_SIZE)), RECORD_SIZE);
                data_copied += RECORD_SIZE;
            }
            else if(cmp_res < 0)
            {
                //Transfer element from B1.
                memcpy((char*) (merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_1_copy + (b1_ind++ * RECORD_SIZE)), RECORD_SIZE);
            }
            else
            {
                //Transfer element from B2.
                memcpy((char*) (merge_addr + (merge_ind++ * RECORD_SIZE)), (char*) (block_2_copy + (b2_ind++ * RECORD_SIZE)), RECORD_SIZE);
            }
        }
        data_copied += RECORD_SIZE;
    }
    
    if(data_copied != final_size)
    {
        return -1;
    }
    if(verbose_flag == 1)
    {
        printf("Total Data Copied to Merge Map: %lu\n", data_copied);
    }
    
    munmap((void*) block_1_copy, block_length_1);
    munmap((void*) block_2_copy, block_length_2);
    return 0;
}
