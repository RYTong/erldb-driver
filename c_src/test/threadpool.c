#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern void io_output(int * arg);

/* async call struct
 *
 */
typedef struct _erl_async {
    struct _erl_async* next;
    struct _erl_async* prev;
    long async_id;
    void* async_data;
    void (*async_invoke)(void*);
    void (*async_free)(void*);
} ErlAsync;

typedef struct {
    struct ThreadList* next;
    pthread_t thr;
} ThreadList;

typedef struct {
    pthread_mutex_t mtx;
    pthread_cond_t cv;
    struct ThreadList* thr_list;
    int len;
    ErlAsync* head;
    ErlAsync* tail;
} AsyncQueue;

typedef struct {
    int key;
    int value;
} AsyncData;

static AsyncQueue* async_q;
static long async_id = 0;

static void* async_main(void*);
static void async_add(ErlAsync*, AsyncQueue*);

int init_async() {
    AsyncQueue* q;
    ThreadList* l;
    int i;
    int max_threads = 3;

    async_q = q = (AsyncQueue*) malloc(sizeof (AsyncQueue));
    q->head = NULL;
    q->tail = NULL;
    q->len = 0;
    pthread_mutex_init(&q->mtx, NULL);
    pthread_cond_init(&q->cv, NULL);
    for (i = 0; i < max_threads; i++) {
        l = (ThreadList*) malloc(sizeof (ThreadList));
        pthread_create(&l->thr, NULL, async_main, (void*) q);
        printf("the created thread === %p \n", &l->thr);
        l->next = q->thr_list;
    }
    q->thr_list = (struct ThreadList*) l;
    return 0;
}

static void async_add(ErlAsync* a, AsyncQueue* q) {
    pthread_mutex_lock(&q->mtx);

    if (q->len == 0) {
        q->head = a;
        q->tail = a;
        q->len = 1;
        pthread_cond_broadcast(&q->cv);
    } else { /* no need to signal (since the worker is working) */
        a->next = q->head;
        q->head->prev = a;
        q->head = a;
        q->len++;
    }
    pthread_mutex_unlock(&q->mtx);
}

static ErlAsync* async_get(AsyncQueue* q) {
    ErlAsync* a;

    pthread_mutex_lock(&q->mtx);
    while ((a = q->tail) == NULL) {
        pthread_cond_wait(&q->cv, &q->mtx);
    }
    if (q->head == q->tail) {
        q->head = q->tail = NULL;
        q->len = 0;
    } else {
        q->tail->prev->next = NULL;
        q->tail = q->tail->prev;
        q->len--;
    }
    pthread_mutex_unlock(&q->mtx);
    return a;
}

static void* async_main(void* arg) {
    AsyncQueue* q = (AsyncQueue*) arg;
    pthread_t main_thread = pthread_self();
    printf("the main_thread1111111 === %p \n", &main_thread);
    while (1) {
        ErlAsync* a = async_get(q);
        printf("the qlen==== %d \n", q->len);
        (*a->async_invoke)(a->async_data);
        free(a);
    }
    return NULL;
}

long driver_async(unsigned int* key,
        void (*async_invoke)(void*), void* async_data,
        void (*async_free)(void*)) {
    ErlAsync* a = (ErlAsync*) malloc(sizeof (ErlAsync));
    long id;

    a->next = NULL;
    a->prev = NULL;
    a->async_data = async_data;
    a->async_invoke = async_invoke;
    a->async_free = async_free;


    async_id = (async_id + 1) & 0x7fffffff;
    if (async_id == 0)
        async_id++;
    id = async_id;
    a->async_id = id;
    async_add(a, async_q);
    return id;
}

void io_test(void * arg) {
    AsyncData* d = (AsyncData*) arg;
    pthread_t main_thread = pthread_self();
    printf("the main_thread2222222 === %p \n", &main_thread);
    printf("the key : %d", d->key);
    io_output(&d->key);
    free(d);
    sleep(2);
}

int main() {
    int i;
    init_async();
    for (i = 0; i < 30; i++) {
        AsyncData* d = (AsyncData*) malloc(sizeof (AsyncData));
        d->key = i;
        d->value = i;
        int c = i;
        driver_async((unsigned int*) & c, io_test, d, NULL);
    }
    printf("the result\n");
    pthread_t main_thread = pthread_self();
    pthread_exit(&main_thread);
    return 0;
}
