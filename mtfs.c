#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <math.h>

void* workerFunc(void* args);
struct mem_block
{
    pthread_t *thread_id;
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
    char* map_addr;
    int file_desc;
    struct stat sb;
    off_t offset, pa_offset;
    size_t file_len, szper;
    int pages_per_block;
    int num_proc, pg_sz;
    int data_sz;


    // Start by retrieving and checking the arguments.
    if(argc != 2)
    {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        exit(1);
    }
    // Open our file and make sure it actually exists.
    if((file_desc = open(argv[1], O_RDONLY)) == -1)
    {
        fprintf(stderr, "There was an issue opening the file.\n");
        exit(2);
    }
    if(fstat(file_desc, &sb) == -1)
    {
        fprintf(stderr, "There was an issue reading the file's size.\n");
        exit(3);
    }

    // Check to make sure the file is actually mappable
    if(sb.st_size == 0)
    {
        fprintf(stderr, "The file must have at least some data in it...\n");
        exit(4);
    }

    // Map file to memory.
    map_addr = mmap(NULL, sb.st_size+1, PROT_READ & PROT_WRITE, MAP_SHARED, file_desc, (off_t)0);
    if(map_addr == NULL)
    {
        fprintf(stderr, "There was a problem mapping the file. \n");
        exit(5);
    }

    // Get the system to tell us how many processors we have.
    num_proc = sysconf(_SC_NPROCESSORS_ONLN);
    pg_sz = sysconf(_SC_PAGE_SIZE);

    // Build our file blocks.
    blocks = calloc(num_proc, sizeof(struct mem_block));
    if(blocks == NULL || sizeof(blocks) != num_proc)
    {
        fprintf(stderr, "Issue allocating memory for thread blocks.\n");
        exit(6);
    }

    // Build our array for threading.
    threads = calloc(num_proc, sizeof(pthread_t));
    if(threads == NULL)
    {
        fprintf(stderr, "There was an issue building the TID array.\n");
        exit(7);
    }

    szper = ceil(sb.st_size/num_proc);
    pages_per_block = ceil((((float)sb.st_size)/num_proc + sb.st_size % num_proc)/pg_sz);
    // ========================== Print out info ===============================
    printf("This system has { %d } processors for use.\n", num_proc);
    printf("This system's page size is: %d\n", pg_sz);
    printf("The input file's size is: %lld\n", sb.st_size);
    printf("Pages per block: %d\n", pages_per_block);
    printf("MAP ADDR: %ld\n", (long)map_addr);
    // =========================================================================

    int block_ind;
    char* current_addr = map_addr;
    int data_count = 0;
    printf("Starting transfer block creation...\n\n");
    for(block_ind = 0; block_ind < num_proc; block_ind++)
    {
        if(block_ind == num_proc - 1)
        {
            blocks[block_ind].start_addr = current_addr;
            blocks[block_ind].length = sb.st_size - data_count;
        }
        else
        {
            blocks[block_ind].start_addr = current_addr;
            blocks[block_ind].length = pages_per_block * pg_sz;
        }
        blocks[block_ind].thread_id = threads[block_ind];
        current_addr += blocks[block_ind].length;
        data_count += blocks[block_ind].length;
        printf("Block[ %d ] | Start Address: %ld | Length: %zu\n", block_ind, (long)blocks[block_ind].start_addr, blocks[block_ind].length);
    }
    printf("\nTotal File Size: %lld, Total Data Encapsulated: %d\n", sb.st_size, data_count);

    pthread_mutex_init(&suspend_mutex, NULL);
    pthread_cond_init(&suspend_cond, NULL);

    for(block_ind = 0; block_ind < num_proc; block_ind++)
    {
        printf("Creating thread %d\n", block_ind);
        if(pthread_create(&threads[block_ind], NULL, workerFunc, (void*)&blocks[block_ind]) != 0)
        {
            fprintf(stderr, "There was a problem creating a thread.\n");
            exit(8);
        }

    }
    pthread_mutex_lock(&suspend_mutex);
    init_flag = 1;
    pthread_cond_broadcast(&suspend_cond);
    pthread_mutex_unlock(&suspend_mutex);
    for(block_ind = 0; block_ind < num_proc; block_ind++)
    {
        pthread_join(threads[block_ind], NULL);
    }

    // Close all of our resources
    pthread_cond_destroy(&suspend_cond);
    pthread_mutex_destroy(&suspend_mutex);
    free(blocks);

    if(munmap(map_addr, sb.st_size) == -1)
    {
        fprintf(stderr, "Could not unmap file\n");
        exit(9);
    }

    return 0;
}
void* workerFunc(void* args)
{
    pthread_mutex_lock(&suspend_mutex);
    while(init_flag == 0)
    {
        pthread_cond_wait(&suspend_cond, &suspend_mutex);
    }
    pthread_mutex_unlock(&suspend_mutex);

    struct mem_block *work_pack = (struct mem_block*) args;
    printf("%ld\n", work_pack->thread_id);
    printf("%ld\n", (long)work_pack->start_addr);
    pthread_exit(0);
}
