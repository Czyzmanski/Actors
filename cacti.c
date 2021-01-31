#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <errno.h>

#include "cacti.h"

#define FINISH_THREADS -1
#define UNUSED(x) (void)(x)

void check_for_successful_alloc(void *data) {
    if (data == NULL) {
        fprintf(stderr, "Allocation failed: %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void mutex_init(pthread_mutex_t *mutex, pthread_mutexattr_t *mutex_attr) {
    if (pthread_mutex_init(mutex, mutex_attr)) {
        fprintf(stderr, "Mutex initialisation failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void mutex_recursive_init(pthread_mutex_t *mutex) {
    pthread_mutexattr_t mutex_attr;
    if (pthread_mutexattr_init(&mutex_attr)) {
        fprintf(stderr, "Mutex attributes initialisation failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
    if (pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE)) {
        fprintf(stderr, "Mutex attributes setting type failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    mutex_init(mutex, &mutex_attr);

    if (pthread_mutexattr_destroy(&mutex_attr)) {
        fprintf(stderr, "Mutex attributes destruction failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void mutex_lock(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex)) {
        fprintf(stderr, "Mutex locking failed: %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void mutex_unlock(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex)) {
        fprintf(stderr, "Mutex unlocking failed: %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void mutex_destroy(pthread_mutex_t *mutex) {
    if (pthread_mutex_destroy(mutex)) {
        fprintf(stderr, "Mutex destruction failed: %d, %s\n", errno,
                strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void cond_init(pthread_cond_t *cond, pthread_condattr_t *cond_attr) {
    if (pthread_cond_init(cond, cond_attr)) {
        fprintf(stderr, "Condition initialisation failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    if (pthread_cond_wait(cond, mutex)) {
        fprintf(stderr, "Waiting on condition failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void cond_signal(pthread_cond_t *cond) {
    if (pthread_cond_signal(cond)) {
        fprintf(stderr, "Signaling on condition failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void cond_destroy(pthread_cond_t *cond) {
    if (pthread_cond_destroy(cond)) {
        fprintf(stderr, "Condition destruction failed: %d, %s\n",
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void thread_create(pthread_t *thread, const pthread_attr_t *attr,
                   void *(*start_routine)(void *), void *arg) {
    if (pthread_create(thread, attr, start_routine, arg)) {
        fprintf(stderr, "Thread creation failed: %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void thread_join(pthread_t thread, void **ret_val) {
    if (pthread_join(thread, ret_val)) {
        fprintf(stderr, "Thread joining failed: %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

typedef struct node node_t;

struct node {
    actor_id_t actor_id;
    node_t *next;
};

typedef struct queue {
    node_t *first;
    node_t *last;
} queue_t;

typedef struct thread_pool {
    queue_t *queue;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_nonempty;
    pthread_key_t key_actor_id;
    pthread_t *threads;
} thread_pool_t;

typedef struct buffer {
    size_t first_pos;
    size_t last_pos;
    size_t size;
    message_t messages[ACTOR_QUEUE_LIMIT];
} buffer_t;

typedef struct actor {
    actor_id_t actor_id;
    bool alive;
    bool scheduled;
    buffer_t *buffer;
    role_t *role;
    void *stateptr;
    pthread_mutex_t mutex;
    pthread_cond_t buffer_space;
} actor_t;

typedef struct sigaction sigaction_t;

typedef struct actor_system {
    bool created;
    thread_pool_t *thread_pool;
    actor_t **actors;
    size_t actors_capacity;
    size_t spawned_actors;
    bool spawning_allowed;
    pthread_mutex_t actors_mutex;
    size_t dead_empty_actors;
    sigaction_t sigaction;
} actor_system_t;

actor_system_t actor_system = {
        .created = false
};

node_t *node_create(actor_id_t actor_id, node_t *next) {
    node_t *node = malloc(sizeof(node_t));
    check_for_successful_alloc(node);
    node->actor_id = actor_id;
    node->next = next;

    return node;
}

void node_destroy(node_t *node) {
    free(node);
}


queue_t *queue_create() {
    queue_t *queue = malloc(sizeof(queue_t));
    check_for_successful_alloc(queue);
    queue->first = NULL;
    queue->last = NULL;

    return queue;
}

bool queue_empty(queue_t *queue) {
    return queue->first == NULL;
}

node_t *queue_pop(queue_t *queue) {
    node_t *node = queue->first;
    queue->first = node->next;
    if (queue_empty(queue)) {
        queue->last = NULL;
    }

    return node;
}

void queue_push(queue_t *queue, actor_id_t actor_id) {
    node_t *node = node_create(actor_id, NULL);
    if (queue_empty(queue)) {
        queue->first = node;
        queue->last = node;
    }
    else {
        queue->last->next = node;
        queue->last = node;
    }
}

void queue_destroy(queue_t *queue) {
    while (!queue_empty(queue)) {
        node_t *node = queue_pop(queue);
        node_destroy(node);
    }
    free(queue);
}


buffer_t *buffer_create() {
    buffer_t *buffer = malloc(sizeof(buffer_t));
    check_for_successful_alloc(buffer);
    buffer->first_pos = 0;
    buffer->last_pos = 0;
    buffer->size = 0;

    return buffer;
}

bool buffer_empty(buffer_t *buffer) {
    return buffer->size == 0;
}

bool buffer_full(buffer_t *buffer) {
    return buffer->size == ACTOR_QUEUE_LIMIT;
}

void buffer_push(buffer_t *buffer, message_t message) {
    buffer->messages[buffer->last_pos] = message;
    buffer->last_pos = (buffer->last_pos + 1) % ACTOR_QUEUE_LIMIT;
    buffer->size++;
}

message_t buffer_pop(buffer_t *buffer) {
    message_t message = buffer->messages[buffer->first_pos];
    buffer->first_pos = (buffer->first_pos + 1) % ACTOR_QUEUE_LIMIT;
    buffer->size--;

    return message;
}

void buffer_destroy(buffer_t *buffer) {
    free(buffer);
}


actor_t *actor_create(actor_id_t actor_id, role_t *role) {
    actor_t *actor = malloc(sizeof(actor_t));
    check_for_successful_alloc(actor);
    actor->actor_id = actor_id;
    actor->alive = true;
    actor->scheduled = false;
    actor->buffer = buffer_create();
    actor->role = role;
    actor->stateptr = NULL;

    mutex_recursive_init(&actor->mutex);
    cond_init(&actor->buffer_space, NULL);

    return actor;
}

void actor_destroy(actor_t *actor) {
    buffer_destroy(actor->buffer);
    mutex_destroy(&actor->mutex);
    cond_destroy(&actor->buffer_space);
    free(actor);
}


actor_id_t actor_system_spawn_actor(role_t *role);

int actor_send_hello_message(actor_id_t actor_id, size_t nbytes, void *data) {
    message_t hello_message = {
            .message_type = MSG_HELLO,
            .nbytes = nbytes,
            .data = data
    };

    return send_message(actor_id, hello_message);
}

void actor_handle_message(actor_t *actor, message_t *message) {
    if (message->message_type == MSG_SPAWN) {
        actor_id_t new_actor = actor_system_spawn_actor(message->data);
        if (new_actor < 0) {
            fprintf(stderr, "%s: failed to spawn a new actor\n", __func__);
        }
        else {
            int err = actor_send_hello_message(
                    new_actor, sizeof(actor_id_t), (void *) actor->actor_id);
            if (err) {
                fprintf(stderr,
                        "%s: failed to send hello to a new actor\n", __func__);
            }
        }
    }
    else if (message->message_type == MSG_GODIE) {
        actor->alive = false;
    }
    else if (message->message_type == MSG_HELLO) {
        actor->role->prompts[0](&actor->stateptr, message->nbytes, message->data);
    }
    else if ((size_t) message->message_type < actor->role->nprompts) {
        actor->role->prompts[message->message_type](&actor->stateptr,
                                                    message->nbytes, message->data);
    }
    else {
        fprintf(stderr, "%s: invalid message type\n", __func__);
    }
}

void actor_schedule_for_execution(actor_id_t actor) {
    mutex_lock(&actor_system.thread_pool->queue_mutex);

    queue_push(actor_system.thread_pool->queue, actor);
    actor_system.actors[actor]->scheduled = true;

    cond_signal(&actor_system.thread_pool->queue_nonempty);
    mutex_unlock(&actor_system.thread_pool->queue_mutex);
}


void *thread_function(void *arg) {
    UNUSED(arg);

    thread_pool_t *thread_pool = actor_system.thread_pool;
    pthread_mutex_t *queue_mutex = &thread_pool->queue_mutex;
    pthread_cond_t *queue_nonempty = &thread_pool->queue_nonempty;

    while (true) {
        mutex_lock(queue_mutex);

        while (queue_empty(thread_pool->queue)) {
            cond_wait(queue_nonempty, queue_mutex);
        }

        node_t *node = queue_pop(thread_pool->queue);
        actor_id_t actor_id = node->actor_id;
        node_destroy(node);

        if (actor_id == FINISH_THREADS) {
            break;
        }

        mutex_unlock(queue_mutex);

        mutex_lock(&actor_system.actors_mutex);
        actor_t *actor = actor_system.actors[actor_id];
        mutex_unlock(&actor_system.actors_mutex);

        mutex_lock(&actor->mutex);

        message_t message = buffer_pop(actor->buffer);

        cond_signal(&actor->buffer_space);
        mutex_unlock(&actor->mutex);

        pthread_setspecific(thread_pool->key_actor_id, &actor->actor_id);
        actor_handle_message(actor, &message);

        mutex_lock(&actor->mutex);

        if (!buffer_empty(actor->buffer)) {
            actor_schedule_for_execution(actor_id);
        }
        else if (!actor->alive) {
            mutex_unlock(&actor->mutex);
            mutex_lock(&actor_system.actors_mutex);

            actor_system.dead_empty_actors++;
            if (actor_system.dead_empty_actors == actor_system.spawned_actors) {
                mutex_lock(queue_mutex);

                for (size_t i = 0; i < POOL_SIZE; i++) {
                    queue_push(actor_system.thread_pool->queue, FINISH_THREADS);
                    cond_signal(queue_nonempty);
                }

                mutex_unlock(queue_mutex);
            }

            mutex_unlock(&actor_system.actors_mutex);
            continue;
        }

        if (buffer_empty(actor->buffer)) {
            actor->scheduled = false;
        }

        mutex_unlock(&actor->mutex);
    }

    mutex_unlock(queue_mutex);

    return NULL;
}

void sigint_handler(int sig) {
    UNUSED(sig);

    mutex_lock(&actor_system.actors_mutex);
    actor_system.spawning_allowed = false;
    mutex_unlock(&actor_system.actors_mutex);

    for (size_t i = 0; i < actor_system.spawned_actors; i++) {
        actor_t *actor = actor_system.actors[i];
        mutex_lock(&actor->mutex);
        actor->alive = false;
        mutex_unlock(&actor->mutex);
    }

    actor_system_join(0);
}

void *thread_signal_handler_function(void *arg) {
    UNUSED(arg);

    int old_type;
    if (pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &old_type)) {
        fprintf(stderr, "%s: pthread_setcanceltype failed, %d, %s\n",
                __func__, errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGINT);
    pthread_sigmask(SIG_UNBLOCK, &block_mask, NULL);

    int sig;
    if (sigwait(&block_mask, &sig)) {
        fprintf(stderr, "%s: sigwait failed, %d, %s\n",
                __func__, errno, strerror(errno));
    }

    return NULL;
}

void thread_pool_create() {
    thread_pool_t *thread_pool = malloc(sizeof(thread_pool_t));
    check_for_successful_alloc(thread_pool);
    actor_system.thread_pool = thread_pool;
    thread_pool->queue = queue_create();

    mutex_init(&thread_pool->queue_mutex, NULL);
    cond_init(&thread_pool->queue_nonempty, NULL);
    if (pthread_key_create(&thread_pool->key_actor_id, NULL)) {
        fprintf(stderr, "%s: pthread_key_create failed, %d, %s\n",
                __func__, errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    thread_pool->threads = malloc(sizeof(pthread_t) * (POOL_SIZE + 1));
    check_for_successful_alloc(thread_pool->threads);

    for (size_t i = 0; i < POOL_SIZE; i++) {
        thread_create(&thread_pool->threads[i], NULL, thread_function, NULL);
    }
    thread_create(&thread_pool->threads[POOL_SIZE], NULL,
                  thread_signal_handler_function, NULL);
}

int thread_pool_join(thread_pool_t *thread_pool) {
    void *ret_val;
    for (size_t i = 0; i < POOL_SIZE; i++) {
        thread_join(thread_pool->threads[i], &ret_val);
    }
    pthread_cancel(thread_pool->threads[POOL_SIZE]);
    thread_join(thread_pool->threads[POOL_SIZE], &ret_val);

    return 0;
}

void thread_pool_destroy(thread_pool_t *thread_pool) {
    queue_destroy(thread_pool->queue);

    mutex_destroy(&thread_pool->queue_mutex);
    cond_destroy(&thread_pool->queue_nonempty);
    if (pthread_key_delete(thread_pool->key_actor_id)) {
        fprintf(stderr, "%s: pthread_key_delete failed, %d, %s\n",
                __func__, errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    free(thread_pool->threads);
    free(thread_pool);
}


int actor_system_init() {
    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &block_mask, NULL);
    actor_system.sigaction.sa_mask = block_mask;
    actor_system.sigaction.sa_handler = sigint_handler;

    int err;
    if ((err = sigaction(SIGINT, &actor_system.sigaction, NULL))) {
        fprintf(stderr, "%s: setting up custom sigaction failed: %d, %s\n",
                __func__, errno, strerror(errno));

        return err;
    }
    else {
        thread_pool_create();

        actor_system.created = true;
        actor_system.actors_capacity = 1024;
        actor_system.actors = malloc(
                actor_system.actors_capacity * sizeof(actor_t *));
        check_for_successful_alloc(actor_system.actors);

        actor_system.spawned_actors = 0;
        actor_system.spawning_allowed = true;
        actor_system.dead_empty_actors = 0;

        mutex_recursive_init(&actor_system.actors_mutex);

        return 0;
    }
}

bool can_spawn_actor() {
    return actor_system.spawning_allowed && actor_system.spawned_actors < CAST_LIMIT;
}

actor_id_t actor_system_spawn_actor(role_t *role) {
    mutex_lock(&actor_system.actors_mutex);

    if (!can_spawn_actor()) {
        mutex_unlock(&actor_system.actors_mutex);

        return -1;
    }
    else {
        if (actor_system.spawned_actors == actor_system.actors_capacity) {
            actor_system.actors_capacity *= 2;
            size_t new_size = actor_system.actors_capacity * sizeof(actor_t *);
            actor_system.actors = realloc(actor_system.actors, new_size);
            check_for_successful_alloc(actor_system.actors);
        }

        actor_id_t actor_id = actor_system.spawned_actors;
        actor_system.actors[actor_id] = actor_create(actor_id, role);
        actor_system.spawned_actors++;
        mutex_unlock(&actor_system.actors_mutex);

        return actor_id;
    }
}

bool actor_system_legal_actor_id(actor_id_t actor) {
    mutex_lock(&actor_system.actors_mutex);
    bool res = 0 <= actor
               && actor < CAST_LIMIT
               && (size_t) actor < actor_system.spawned_actors;
    mutex_unlock(&actor_system.actors_mutex);

    return res;
}

void actor_system_dispose() {
    actor_system.created = false;
    thread_pool_destroy(actor_system.thread_pool);

    for (size_t i = 0; i < actor_system.spawned_actors; i++) {
        actor_destroy(actor_system.actors[i]);
    }
    free(actor_system.actors);

    actor_system.spawned_actors = 0;
    actor_system.dead_empty_actors = 0;

    mutex_destroy(&actor_system.actors_mutex);
}

actor_id_t actor_id_self() {
    actor_id_t *actor_id = pthread_getspecific(
            actor_system.thread_pool->key_actor_id);

    return *actor_id;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    int err;
    if ((err = actor_system_init())) {
        return err;
    }
    else {
        *actor = actor_system_spawn_actor(role);
        if (*actor < 0) {
            return -1;
        }
        else {
            if ((err = actor_send_hello_message(*actor, 0, NULL))) {
                fprintf(stderr, "%s: failed to send hello\n", __func__);
            }

            return err;
        }
    }
}

void actor_system_join(actor_id_t actor) {
    if (actor_system.created) {
        if (!actor_system_legal_actor_id(actor)) {
            fprintf(stderr, "%s: invalid actor id\n", __func__);
        }
        else {
            thread_pool_join(actor_system.thread_pool);
            actor_system_dispose();
        }
    }
}

int send_message(actor_id_t actor, message_t message) {
    if (!actor_system_legal_actor_id(actor)) {
        return -2;
    }
    else {
        buffer_t *actor_buffer = actor_system.actors[actor]->buffer;
        pthread_mutex_t *actor_mutex = &actor_system.actors[actor]->mutex;
        pthread_cond_t *actor_cond = &actor_system.actors[actor]->buffer_space;

        mutex_lock(actor_mutex);

        if (!actor_system.actors[actor]->alive) {
            mutex_unlock(actor_mutex);

            return -1;
        }
        else {
            while (buffer_full(actor_buffer)) {
                cond_wait(actor_cond, actor_mutex);
            }

            bool schedule_actor = buffer_empty(actor_buffer)
                                  && !actor_system.actors[actor]->scheduled;
            buffer_push(actor_buffer, message);

            if (schedule_actor) {
                actor_schedule_for_execution(actor);
            }

            mutex_unlock(actor_mutex);

            return 0;
        }
    }
}
