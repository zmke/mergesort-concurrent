#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <stdatomic.h>

#include "threadpool.h"
#include "list.h"
#include "merge_sort.h"

#define USAGE "usage: ./sort [thread_count] [input_file]\n"

#define MIN(a, b) (((a) < (b)) ? (a) : (b))

struct {
    pthread_mutex_t mutex;
} data_context;

static llist_t *tmp_list;
static llist_t *the_list = NULL;

static int thread_count = 0, data_count = 0, max_cut = 0;
static tpool_t *pool = NULL;

static int local_size = 0, last_local_size = 0;

typedef struct {
    llist_t *source;
    llist_t *dest;
    int task_cnt;
} merge_list_arg_t;

void merge_thread_lists(void *data);

node_t *list_pop(llist_t *list)
{
    node_t *head = list->head;
    node_t *tail = list->tail;
    node_t *node, *node_next;
    do {
        node = head->next;
        if (node == tail)
            return NULL;
        node_next = node->next;
        if (atomic_compare_exchange_weak(&(head->next), &node, node_next))
            break;
    } while (1);
    return node;
}

node_t *list_search(node_t *cur, node_t *tail, val_t data, node_t** left_node)
{
    node_t *right_node;
    node_t *t = cur;
    node_t *t_next = cur->next;
    do {
        *(left_node) = t;
        t = t_next;
        if (t == tail)
            break;
        t_next = t->next;
    } while (strcmp((char *)t->data, (char *)data) <= 0);
    right_node = t;
    return right_node;
}

void list_insert(llist_t *list, node_t **cur, node_t * node)
{
    node_t *left_node, *right_node;
    do {
        right_node = list_search(*cur, list->tail, node->data, &left_node);
        node->next = right_node;
        if (atomic_compare_exchange_weak(&(left_node->next), &right_node, node)) {
            *cur = node;
            return;
        }
    } while (1);
}

void concurrent_merge(void *data)
{
    merge_list_arg_t *arg = (merge_list_arg_t *)data;
    llist_t *source = arg->source;
    llist_t *dest = arg->dest;
    node_t *cur = NULL, *node = NULL;
    int old_cnt = 0;
    cur = dest->head;
    while (1) {
        node = list_pop(source);
        if (!node) {
            old_cnt = atomic_fetch_sub(&(arg->task_cnt), 1);
            if (old_cnt == 1) {
                dest->size += source->size;
                source->size = 0;
                list_free_nodes(source);
                tqueue_push(pool->queue, task_new(merge_thread_lists, dest));
            }
            break;
        }
        list_insert(dest, &cur, node);
    }
}

void merge_thread_lists(void *data)
{
    llist_t *_list = (llist_t *) data;
    if (_list->size < (uint32_t) data_count) {
        pthread_mutex_lock(&(data_context.mutex));
        llist_t *_t = tmp_list;
        if (!_t) {
            tmp_list = _list;
            pthread_mutex_unlock(&(data_context.mutex));
        } else {
            /*
             * If there is a local list left by other thread,
             * pick it and create a task to merge the picked list
             * and its own local list.
             */
            tmp_list = NULL;
            pthread_mutex_unlock(&(data_context.mutex));

            merge_list_arg_t *arg = malloc(sizeof(merge_list_arg_t));
            if (_t->size < _list->size) {
                arg->source = _t;
                arg->dest = _list;
            } else {
                arg->source = _list;
                arg->dest = _t;
            }

            arg->task_cnt = arg->source->size / local_size * 2;
            int task_cnt = arg->task_cnt;
            for (int i = 0; i < task_cnt; i++)
                tqueue_push(pool->queue, task_new(concurrent_merge, arg));
        }
    } else {
        /*
         * All local lists are merged, push a termination task to task queue.
         */
        the_list = _list;
        tqueue_push(pool->queue, task_new(NULL, NULL));
    }
}

void sort_local_list(void *data)
{
    llist_t *local_list = (llist_t *) data;
    merge_thread_lists(merge_sort(local_list));
}

void cut_local_list(void *data)
{
    llist_t *list = (llist_t *) data, *local_list;
    node_t *head_next, *tail_prev;
    local_size = data_count / max_cut;
    last_local_size = list->size - local_size * (max_cut - 1);

    head_next = list->head->next;
    for (int i = 0; i < max_cut - 1; ++i) {
        /* Create local list container */
        local_list = list_new();
        local_list->head->next = head_next;
        local_list->size = local_size;
        /* Cut the local list */
        tail_prev = list_get(local_list, local_size - 1);
        head_next = tail_prev->next;
        tail_prev->next = local_list->tail;
        /* Create new task */
        tqueue_push(pool->queue, task_new(sort_local_list, local_list));
    }
    /* The last takes the rest. */
    local_list = list_new();
    local_list->head->next = head_next;
    local_list->size = last_local_size;
    tail_prev = list_get(local_list, last_local_size - 1);
    tail_prev->next = local_list->tail;
    list->head->next = list->tail;
    list_free_nodes(list);
    tqueue_push(pool->queue, task_new(sort_local_list, local_list));
}

static void *task_run(void *data __attribute__ ((__unused__)))
{
    while (1) {
        pthread_mutex_lock(&(pool->queue->mutex));
        while(pool->queue->size == 0)
            pthread_cond_wait(&(pool->queue->cond), &(pool->queue->mutex));
        pthread_mutex_unlock(&(pool->queue->mutex));
        task_t *_task = tqueue_pop(pool->queue);
        if (_task) {
            if (!_task->func) {
                tqueue_push(pool->queue, task_new(NULL, NULL));
                free(_task);
                break;
            } else {
                _task->func(_task->arg);
                free(_task);
            }
        }
    }
    pthread_exit(NULL);
}

static uint32_t build_list_from_file(llist_t *_list, const char *filename)
{
    FILE *fp = fopen(filename, "r");
    char buffer[16];

    while (fgets(buffer, 16, fp) != NULL) {
        char *name = (char*)malloc(16);
        strcpy(name, buffer);
        list_add(_list, (val_t)name);
    }

    fclose(fp);
    return _list->size;
}

int main(int argc, char const *argv[])
{
    if (argc < 3) {
        printf(USAGE);
        return -1;
    }
    thread_count = atoi(argv[1]);

    /* Read data */
    the_list = list_new();
    data_count = build_list_from_file(the_list, argv[2]);

    max_cut = MIN(thread_count, data_count);

    /* initialize tasks inside thread pool */
    pthread_mutex_init(&(data_context.mutex), NULL);
    tmp_list = NULL;
    pool = (tpool_t *) malloc(sizeof(tpool_t));
    tpool_init(pool, thread_count, task_run);

    struct timeval start, end;
    uint32_t consumed_tasks;
    double duration;
    /* Start when the first task launches. */
    gettimeofday(&start, NULL);

    /* launch the first task */
    tqueue_push(pool->queue, task_new(cut_local_list, the_list));
    /* release thread pool */
    consumed_tasks = tpool_free(pool);

    gettimeofday(&end, NULL);

    /* Report */
    duration = (end.tv_sec - start.tv_sec) * 1000 +
               (double)(end.tv_usec - start.tv_usec) / 1000.0f;
    printf("#Total_tasks_consumed: %d\n", consumed_tasks);
    printf("#Elapsed_time: %.3lf ms\n", duration);
    printf("#Throughput: %d (per sec)\n", (uint32_t)(consumed_tasks * 1000 / duration));

    /* Output sorted result */
    list_print(the_list);
    FILE *output;
    output = fopen("output.txt", "a+");
    fprintf(output, "%lf\n", duration);
    fclose(output);
    return 0;
}
