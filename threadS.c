
#include "sut.h"
#include "queue/queue.h"
#include <pthread.h>
#include <ucontext.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>



// assume stack size is 1024*64
// each user thread will include sut_exit() at the end or middle
// before read or write we always open the file
// if the file doesn't exist, we will return -ve value of fd according to the assignment description
#define STACK_SIZE 1024 * 64

typedef enum {
    IO_OPEN,   
    IO_READ,   
    IO_WRITE,  
    IO_CLOSE   
} io_operation_t;


typedef struct {
    ucontext_t context; 
    io_operation_t req_type;   
    int fd;    
    char *buffer; 
    char *file_path; 
    int size;   // read/write size
    ssize_t bytes_read;     
    ssize_t bytes_written;    
} sut_task_t;


// global variables
sut_task_t* current_task = NULL;
// queue for C-exec
struct queue ready_queue;
// queue for I-exec
struct queue wait_queue;

//two kernel-level threads
ucontext_t c_exec_context;
ucontext_t i_exec_context;
pthread_t c_exec_thread;
pthread_t i_exec_thread;


bool toshutdown = false;
//mutex and cond for two queues
pthread_mutex_t compute_queue_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t compute_queue_cond=PTHREAD_COND_INITIALIZER;
pthread_mutex_t wait_queue_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t wait_queue_cond=PTHREAD_COND_INITIALIZER;


///////// signal for no active task
int active_task_count = 0;
pthread_mutex_t task_count_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t no_active_tasks_cond = PTHREAD_COND_INITIALIZER;
bool c_exec_end = false;


///////// signal for c-exec starts//////////
pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t start_cond = PTHREAD_COND_INITIALIZER;
bool thread_started = false;


pthread_mutex_t iostart_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t iostart_cond = PTHREAD_COND_INITIALIZER;
bool iothread_started = false;

// if sut_create is called count++
void increment_task_count() {
    pthread_mutex_lock(&task_count_mutex);
    active_task_count++;
    pthread_mutex_unlock(&task_count_mutex);
}

// if sut_exit is called count--
void decrement_task_count() {
    pthread_mutex_lock(&task_count_mutex);
    active_task_count--;
    pthread_mutex_unlock(&task_count_mutex);
}

// c-exec thread
void* c_exec_function(void* arg) {
    //if c-exec thread starts, signal the init thread
    pthread_mutex_lock(&start_mutex);
    thread_started = true;
    
    pthread_cond_signal(&start_cond);
    pthread_mutex_unlock(&start_mutex);
    //used for nanosleep
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000; 
    // infinite loop, unless we break out of it
    while (true) {   
        
        pthread_mutex_lock(&compute_queue_mutex); 
        //in case ready queue is empty   
        while (queue_peek_front(&ready_queue) == NULL)  {       
            pthread_mutex_unlock(&compute_queue_mutex); 
            nanosleep(&ts, NULL); 
            pthread_mutex_lock(&compute_queue_mutex); 
        }
        // Grab the first task from the ready queue
        struct queue_entry *entry = queue_pop_head(&ready_queue);
        pthread_mutex_unlock(&compute_queue_mutex); 
        sut_task_t *task = (sut_task_t *)entry->data; 
        current_task=task; 
        swapcontext(&c_exec_context, &current_task->context);
        
        pthread_mutex_lock(&task_count_mutex);
        if (active_task_count == 0&&toshutdown==true) { 
           
            c_exec_end = true;
            break;
        }
        pthread_mutex_unlock(&task_count_mutex);

       
    }
   
    return NULL;
}



void* i_exec_function(void *arg){
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000; 
    //if i-exec thread starts, signal the init thread
    pthread_mutex_lock(&iostart_mutex);
    iothread_started = true;
    pthread_cond_signal(&iostart_cond);
    pthread_mutex_unlock(&iostart_mutex);
   
    while(true){
       
        pthread_mutex_lock(&wait_queue_mutex);
        while (queue_peek_front(&wait_queue) == NULL&&!c_exec_end){   
             pthread_mutex_unlock(&wait_queue_mutex);
             nanosleep(&ts, NULL);
             pthread_mutex_lock(&wait_queue_mutex); 
        };
         if (c_exec_end&&toshutdown==true) {
            break;
        }  
        struct queue_entry *entry = queue_pop_head(&wait_queue);
        pthread_mutex_unlock(&wait_queue_mutex); 

        sut_task_t *task = (sut_task_t *)entry->data;
        switch (task->req_type) {
        case IO_OPEN:
        //we don't know if we need to read or write, so O_RDWR mode.
            task->fd = open(task->file_path, O_RDWR|O_CREAT,0666);
            break;
        case IO_READ:
            task->bytes_read = read(task->fd, task->buffer, task->size);
            break;
        case IO_WRITE:
            task->bytes_written = write(task->fd, task->buffer, task->size);
            break;
        case IO_CLOSE:
            close(task->fd);
            break;
       
        }   
    
   
    struct queue_entry* entrytask = queue_new_node(task);
    pthread_mutex_lock(&compute_queue_mutex);
    queue_insert_tail(&ready_queue, entrytask);  
    pthread_mutex_unlock(&compute_queue_mutex);
    }
   
}


int sut_open(char *file_name){
    current_task->req_type = IO_OPEN;
    current_task->file_path = strdup(file_name);
    pthread_mutex_lock(&wait_queue_mutex);
    struct queue_entry* entry = queue_new_node(current_task);
    queue_insert_tail(&wait_queue, entry); 
    pthread_mutex_unlock(&wait_queue_mutex);
    swapcontext(&current_task->context, &c_exec_context);
    //fd will be returned
    int fd = current_task->fd;
    return fd;
 
}

char* sut_read(int fd, char *buf, int size){
    current_task->req_type = IO_READ;
    current_task->fd = fd;
    current_task->buffer = buf;
    current_task->size = size;
    
    pthread_mutex_lock(&wait_queue_mutex);
    struct queue_entry* entry = queue_new_node(current_task);
    queue_insert_tail(&wait_queue, entry);  
    pthread_mutex_unlock(&wait_queue_mutex);
    swapcontext(&current_task->context, &c_exec_context);
    // the content of the file will be returned
    char* result = current_task->buffer;

    return result;


}

void sut_write(int fd, char *buf, int size){
   
    // assign the necessary values to the task
    current_task->req_type = IO_WRITE;
    current_task->fd = fd;
    current_task->buffer = buf;
    current_task->size = size;

    pthread_mutex_lock(&wait_queue_mutex);
    struct queue_entry* entry = queue_new_node(current_task);
    queue_insert_tail(&wait_queue, entry);  
    pthread_mutex_unlock(&wait_queue_mutex);
    // printf("%s\n",current_task->buffer);
    swapcontext(&current_task->context, &c_exec_context);
    
}


void sut_close(int fd){
    
    current_task->req_type = IO_CLOSE;
    current_task->fd = fd;
    pthread_mutex_lock(&wait_queue_mutex);
    struct queue_entry* entry = queue_new_node(current_task);
    queue_insert_tail(&wait_queue, entry);  
    pthread_mutex_unlock(&wait_queue_mutex);
    swapcontext(&current_task->context, &c_exec_context);
    
}



void sut_init() {
   
    // two queues to record tasks
    ready_queue = queue_create();
    wait_queue = queue_create();
    queue_init(&ready_queue);
    queue_init(&wait_queue);
    //just in case any error happens
    if (pthread_create(&c_exec_thread, NULL, c_exec_function, NULL) != 0) {
            perror("Failed to create C-EXEC thread");
            exit(EXIT_FAILURE);
    }
    if (pthread_create(&i_exec_thread, NULL, i_exec_function, NULL) != 0) {
            perror("Failed to create C-EXEC thread");
            exit(EXIT_FAILURE);
    }
    // Wait for the compute executor thread to start
    pthread_mutex_lock(&start_mutex);
   
    while (!thread_started) {
         
        pthread_cond_wait(&start_cond, &start_mutex);
    }
    pthread_mutex_unlock(&start_mutex);
    // Wait for the io executor thread to start
    pthread_mutex_lock(&iostart_mutex);
    while (!iothread_started) {
        pthread_cond_wait(&iostart_cond, &iostart_mutex);
    }
    pthread_mutex_unlock(&iostart_mutex);
    

}


sut_task_t* create_task(sut_task_f fn) {
    sut_task_t* task = (sut_task_t*)malloc(sizeof(sut_task_t));
    // // Initialize the task's context
    getcontext(&task->context);
    // Allocate a stack for the new task
    task->context.uc_stack.ss_sp = malloc(STACK_SIZE);
    task->context.uc_stack.ss_size = STACK_SIZE;
    task->context.uc_stack.ss_flags = 0;
    task->context.uc_link = &c_exec_context; 
    //pass fn to the context
    makecontext(&task->context, (void (*)())fn, 0);
    return task;
}



bool sut_create(sut_task_f fn) {
    pthread_mutex_lock(&compute_queue_mutex);
    sut_task_t* new_task = create_task(fn);
    if (new_task == NULL) {
        pthread_mutex_unlock(&compute_queue_mutex);
        return false;
    }
    struct queue_entry* entry = queue_new_node(new_task);
    queue_insert_tail(&ready_queue, entry);
    //new task, so ++
    increment_task_count();
    pthread_mutex_unlock(&compute_queue_mutex);
    return true;
}


void sut_yield() {
    // stop current task, put it to the end of the ready queue
    pthread_mutex_lock(&compute_queue_mutex);
    struct queue_entry* entry = queue_new_node(current_task);
    queue_insert_tail(&ready_queue, entry);
    pthread_mutex_unlock(&compute_queue_mutex);
    // go to c-exec
    swapcontext(&current_task->context, &c_exec_context);
    
}


void sut_exit() {
    //free memory
    free(current_task);
    current_task = NULL;
    //count--
    decrement_task_count();
    //go back c-exec
    setcontext(&c_exec_context);
}




void sut_shutdown() {
    toshutdown = true;
   //it seems we assume no failure, but i set perror here just in case.
    if (pthread_join(i_exec_thread, NULL) != 0) {
        perror("Failed to join I-EXEC thread");
    }
    if (pthread_join(c_exec_thread, NULL) != 0) {
        perror("Failed to join C-EXEC thread");
    }
    pthread_mutex_destroy(&compute_queue_mutex);
    pthread_mutex_destroy(&wait_queue_mutex);
    pthread_cond_destroy(&compute_queue_cond);
    pthread_cond_destroy(&wait_queue_cond);
    pthread_mutex_destroy(&task_count_mutex);
    pthread_cond_destroy(&no_active_tasks_cond);
    pthread_cond_destroy(&iostart_cond);
    pthread_mutex_destroy(&iostart_mutex);
    pthread_cond_destroy(&start_cond);
    pthread_mutex_destroy(&start_mutex);
    
    
    
}





















