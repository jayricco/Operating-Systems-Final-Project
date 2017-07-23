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

void print_array(const char* start, const size_t len);
void* workerFunc(void* args);
int cmpfunc (const void * a, const void * b);

struct mem_block
{
    int thread_id;
    int slave_tid;
    char* start_addr;
    size_t length;
};

struct mem_block *blocks;
pthread_t *threads;

pthread_cond_t suspend_cond;
pthread_mutex_t suspend_mutex;
int init_flag= 0;

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
                fprintf(stderr, "Usage: %s -[v, a, n <num_threads>] <filename>\n", argv[0]);
            }
        }
        else
        {
            fprintf(stderr, "Usage: %s -[v, a, n <num_threads>] <filename>\n", argv[0]);
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
            }
            else if (strcmp(argv[i], "-n") == 0)
            {
                thread_override = atoi(argv[i+1]);
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
    // ========================== Print out info ===============================
    printf("==================================================================\n");
    printf("This system has (%d) processors for use\n", num_proc_onln);
    printf("This system's page size is: %d\n", page_sz);
    printf("The input file's size is: %lld\n", file_stats.st_size);
    printf("We are able to utilize %d threads\n", num_threads);
    printf("Pages Per Block: %d\n", pages_per_block);
    printf("Maximum # of pages: %d\n", num_pages);
    printf("Map Starting Address: %ld\n", (long)map_addr);
    
    printf("Map First Record: \" ");
    int ind;
    // Subtracting 2 from RECORD_SIZE to get rid of CL and RF characters.
    for(ind = 0; ind < RECORD_SIZE-2; ind++)
    {
        printf("%c", map_addr[ind]);
    }
    printf(" \"\n");
    printf("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n\n");
    // =========================================================================
    
    
    int block_ind;
    char* current_addr = map_addr;
    int data_count = 0;

    printf("Starting Transfer Block Creation...\n");
    printf("==================================================================\n");
    
    for(block_ind = 0; block_ind < num_threads; block_ind++)
    {
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
            blocks[block_ind].slave_tid = block_ind+1;
        }
        else
        {
            blocks[block_ind].slave_tid = -1;
        }
        current_addr += blocks[block_ind].length;
        data_count += szper;
        
        printf("Block[ %d ] | Start Address: %lx | Length: %zu\n", block_ind, (unsigned long)blocks[block_ind].start_addr, blocks[block_ind].length);
    }
    
    printf("\nTotal File Size: %lld, Total Data Encapsulated: %d\n", file_stats.st_size, data_count);
    printf("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n\n");
    
    pthread_mutex_init(&suspend_mutex, NULL);
    pthread_cond_init(&suspend_cond, NULL);
    int threads_to_create = num_threads;
    int skip = 1; // This is the size of the jump iterating over the thread id's.
    printf("Starting MergeSort...\n");
    printf("==================================================================\n");
    struct timespec time_start, time_end;
    clock_gettime(CLOCK_REALTIME, &time_start);
    
    while(skip <= num_threads)
    {
        printf("Number Of Threads To Be Created: %d\n", threads_to_create);
        
        //Set our wait condition for the threads.
        pthread_mutex_lock(&suspend_mutex);
        init_flag = 0;
        pthread_cond_broadcast(&suspend_cond);
        pthread_mutex_unlock(&suspend_mutex);
        
        // Create threads according to skip
        printf("Creating Thread: ");
        for(block_ind = 0; block_ind < num_threads; block_ind += skip)
        {
            
            printf("%d.. ", block_ind);
            if(pthread_create(&threads[block_ind], NULL, workerFunc, (void*)&blocks[block_ind]) != 0)
            {
                fprintf(stderr, "\nThere was a problem creating a thread.\n");
                exit(8);
            }
        }
        printf("\n");
        // Once they've been created - let 'em rip!
        pthread_mutex_lock(&suspend_mutex);
        init_flag = 1;
        pthread_cond_broadcast(&suspend_cond);
        pthread_mutex_unlock(&suspend_mutex);
        
        // Since every other thread is a "slave", multiply the skip by 2 because we only need to re-join with the master threads.
        skip *= 2;
        
        //In this first for-loop, double the length's for /all/ master threads.
        for(block_ind = 0; block_ind < num_threads; block_ind += skip)
        {
            pthread_join(threads[block_ind], NULL);
            blocks[block_ind].length += blocks[blocks[block_ind].slave_tid].length;
        }

        //Now, we want to double ourselves again, and every other previous master now becomes a slave.
        //i.e. going from (Master, Slave) combinations of (0, 1), (2, 3) to (0, 2).
        for(block_ind = 0; block_ind < num_threads; block_ind += skip)
        {
            if(block_ind % (skip*2) == 0)
            {
                if(block_ind + skip >= num_threads)
                {
                    blocks[block_ind].slave_tid = -2;
                }
                else
                {
                    blocks[block_ind].slave_tid = (block_ind + skip);
                    blocks[block_ind + skip].slave_tid = -1;
                }
            }
        }
        threads_to_create = (int)floor(threads_to_create/2.0);
        if(skip <= num_threads)
        {
            printf("------------------------------------------------------------------\n\n");
        }
        else
        {
            printf("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n\n");
            
        }
    }
    clock_gettime(CLOCK_REALTIME, &time_end);
    float total_time = (time_end.tv_sec + ((float)time_end.tv_nsec / 1000000000.0)) - (time_start.tv_sec + ((float)time_start.tv_nsec / 1000000000.0));
    printf("Total Time Taken: %f sec.\n", total_time);
    
    //printf("Printing Final, Sorted Array Keys: \n");
    //print_array(map_addr, file_stats.st_size);
    //printf("\n");
    
    // Close all of our resources
    
    pthread_cond_destroy(&suspend_cond);
    pthread_mutex_destroy(&suspend_mutex);
    free(threads);
    free(blocks);
    munmap(map_addr, file_stats.st_size);
    fclose(file);
    
    return 0;
}

int cmpfunc (const void * a, const void * b)
{
    const char * pa = (const char *) a;
    const char * pb = (const char *) b;
    return strncmp(pa, pb, 8);
}
void print_array(const char* start, const size_t len)
{
    char key[8];
    size_t i;
    printf("[ ");
    for(i = 0; i < len; i+=RECORD_SIZE)
    {
        bcopy(start+i, key, 7);
        if(i + RECORD_SIZE >= len)
        {
            printf("%s", key);
        }
        else
        {
            printf("%s, ", key);
        }
    }
    printf(" ]\n");
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
    // Do the sort!
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time_start);
    qsort(work_pack->start_addr, work_pack->length/RECORD_SIZE, RECORD_SIZE, cmpfunc);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time_finish);
    float qsort_time = (time_finish.tv_sec + ((float)time_finish.tv_nsec / 1000000000.0)) - (time_start.tv_sec + ((float)time_start.tv_nsec / 1000000000.0));
    // slave_tid is only set to -2 on the last, final merge of the algorithm.
    if(work_pack->slave_tid == -2)
    {
        printf("FINAL Master[ %d ] - Start Addr: %lx, Length: %zu, Time: %f sec\n", work_pack->thread_id, (unsigned long)work_pack->start_addr, work_pack->length, qsort_time);
    }
    else if(work_pack->slave_tid != -1)
    {
        printf("Master[ %d ] - Slave: %d, Start Addr: %lx, Length: %zu, Time: %f sec\n", work_pack->thread_id,  work_pack->slave_tid, (unsigned long)work_pack->start_addr, work_pack->length, qsort_time);
        pthread_join(threads[work_pack->slave_tid], NULL);
    }
    else
    {
        printf("Slave[ %d ] - Start Addr: %lx, Length: %zu, Time: %f sec\n", (work_pack->thread_id), (unsigned long)work_pack->start_addr, work_pack->length, qsort_time);

    }
    pthread_exit(0);
}
