#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <assert.h>

#include "cacti.h"

#define FINISH_THREADS -1

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

#define BUFFER_SIZE 1024

typedef struct buffer {
    size_t first_pos;
    size_t last_pos;
    size_t size;
    message_t messages[BUFFER_SIZE];
} buffer_t;

typedef struct actor {
    actor_id_t actor_id;
    bool alive;
    buffer_t *buffer;
    role_t *role;
    void *stateptr;
    pthread_mutex_t mutex;
    pthread_cond_t buffer_space;
} actor_t;

typedef struct sigaction sigaction_t;

typedef struct actor_system {
    thread_pool_t *thread_pool;
    actor_t *actors[CAST_LIMIT];
    size_t spawned_actors;
    bool spawning_allowed;
    pthread_mutex_t actors_mutex;
    size_t dead_empty_actors;
    sigaction_t sigaction;
} actor_system_t;

actor_system_t actor_system;


node_t *node_create(actor_id_t actor_id, node_t *next) {
    node_t *node = malloc(sizeof(node_t));
    if (node == NULL) {
        exit(EXIT_FAILURE);
    }

    node->actor_id = actor_id;
    node->next = next;

    return node;
}

void node_destroy(node_t *node) {
    free(node);
}


queue_t *queue_create() {
    queue_t *queue = malloc(sizeof(queue_t));
    if (queue == NULL) {
        exit(EXIT_FAILURE);
    }

    queue->first = NULL;
    queue->last = NULL;

    return queue;
}

bool queue_empty(queue_t *queue) {
    return queue->first == NULL;
}

node_t *queue_pop(queue_t *queue) {
    assert(!queue_empty(queue));
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
    if (buffer == NULL) {
        exit(EXIT_FAILURE);
    }
    buffer->first_pos = 0;
    buffer->last_pos = 0;
    buffer->size = 0;

    return buffer;
}

bool buffer_empty(buffer_t *buffer) {
    return buffer->size == 0;
}

bool buffer_full(buffer_t *buffer) {
    return buffer->size == BUFFER_SIZE;
}

void buffer_push(buffer_t *buffer, message_t message) {
    buffer->messages[buffer->last_pos] = message;
    buffer->last_pos = (buffer->last_pos + 1) % BUFFER_SIZE;
    buffer->size++;
}

message_t buffer_pop(buffer_t *buffer) {
    assert(!buffer_empty(buffer));
    message_t message = buffer->messages[buffer->first_pos];
    buffer->first_pos = (buffer->first_pos + 1) % BUFFER_SIZE;
    buffer->size--;

    return message;
}

void buffer_destroy(buffer_t *buffer) {
    free(buffer);
}


actor_t *actor_create(actor_id_t actor_id, role_t *role) {
    actor_t *actor = malloc(sizeof(actor_t));
    if (actor == NULL) {
        exit(EXIT_FAILURE);
    }
    actor->actor_id = actor_id;
    actor->alive = true;
    actor->buffer = buffer_create();
    actor->role = role;
    actor->stateptr = NULL;

    pthread_mutexattr_t mutex_attr;
    if (pthread_mutexattr_init(&mutex_attr)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&actor->mutex, &mutex_attr)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&actor->buffer_space, NULL)) {
        exit(EXIT_FAILURE);
    }

    return actor;
}

void actor_destroy(actor_t *actor) {
    buffer_destroy(actor->buffer);
    if (pthread_mutex_destroy(&actor->mutex)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_destroy(&actor->buffer_space)) {
        exit(EXIT_FAILURE);
    }
    free(actor->stateptr);
    free(actor);
}


actor_id_t actor_system_spawn_actor(role_t *role);

void actor_handle_message(actor_t *actor, message_t *message) {
    if (message->message_type == MSG_SPAWN) {
        actor_id_t new_actor = actor_system_spawn_actor(message->data);
        if (new_actor < 0) {
            //TODO: error handling
        }
        else {
            message_t hello_message;
            hello_message.message_type = MSG_HELLO;
            hello_message.nbytes = sizeof(actor->actor_id);
            hello_message.data = &actor->actor_id;

            if (send_message(new_actor, hello_message)) {
                //TODO: error handling
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
}

void actor_schedule_for_execution(actor_id_t actor) {
    if (pthread_mutex_lock(&actor_system.thread_pool->queue_mutex)) {
        exit(EXIT_FAILURE);
    }

    queue_push(actor_system.thread_pool->queue, actor);

    if (pthread_cond_signal(&actor_system.thread_pool->queue_nonempty)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_unlock(&actor_system.thread_pool->queue_mutex)) {
        exit(EXIT_FAILURE);
    }
}


void *thread_function(void *arg) {
    thread_pool_t *thread_pool = actor_system.thread_pool;
    pthread_mutex_t *queue_mutex = &thread_pool->queue_mutex;
    pthread_cond_t *queue_nonempty = &thread_pool->queue_nonempty;

    while (true) {
        if (pthread_mutex_lock(queue_mutex)) {
            exit(EXIT_FAILURE);
        }

        while (queue_empty(thread_pool->queue)) {
            if (pthread_cond_wait(queue_nonempty, queue_mutex)) {
                exit(EXIT_FAILURE);
            }
        }

        node_t *node = queue_pop(thread_pool->queue);
        actor_id_t actor_id = node->actor_id;
        node_destroy(node);

        if (actor_id == FINISH_THREADS) {
            break;
        }

        if (pthread_mutex_unlock(queue_mutex)) {
            exit(EXIT_FAILURE);
        }

        actor_t *actor = actor_system.actors[actor_id];
        pthread_setspecific(thread_pool->key_actor_id, &actor->actor_id);

        if (pthread_mutex_lock(&actor->mutex)) {
            exit(EXIT_FAILURE);
        }

        message_t message = buffer_pop(actor->buffer);
        actor_handle_message(actor, &message);

        if (!buffer_empty(actor->buffer)) {
            actor_schedule_for_execution(actor_id);
        }
        else if (!actor->alive) {
            if (pthread_mutex_lock(&actor_system.actors_mutex)) {
                exit(EXIT_FAILURE);
            }

            actor_system.dead_empty_actors++;
            if (actor_system.dead_empty_actors == actor_system.spawned_actors) {
                if (pthread_mutex_lock(queue_mutex)) {
                    exit(EXIT_FAILURE);
                }

                for (size_t i = 0; i < POOL_SIZE; i++) {
                    queue_push(actor_system.thread_pool->queue, FINISH_THREADS);
                }

                if (pthread_cond_broadcast(queue_nonempty)) {
                    exit(EXIT_FAILURE);
                }
                if (pthread_mutex_unlock(queue_mutex)) {
                    exit(EXIT_FAILURE);
                }
            }

            if (pthread_mutex_unlock(&actor_system.actors_mutex)) {
                exit(EXIT_FAILURE);
            }
        }

        if (pthread_mutex_unlock(&actor->mutex)) {
            exit(EXIT_FAILURE);
        }
    }

    if (pthread_mutex_unlock(queue_mutex)) {
        exit(EXIT_FAILURE);
    }

    return NULL;
}

void sigint_handler(int sig) {
    if (pthread_mutex_lock(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }

    actor_system.spawning_allowed = false;

    if (pthread_mutex_unlock(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < actor_system.spawned_actors; i++) {
        message_t message_go_die;
        message_go_die.message_type = MSG_GODIE;
        send_message(i, message_go_die);
    }

    actor_system_join(0);
}

void *thread_signal_handler_function(void *arg) {
    int sig;
    int err = sigwait(&actor_system.sigaction.sa_mask, &sig);
    if (err != 0) {
        //TODO: error handling
    }

    return NULL;
}

thread_pool_t *thread_pool_create() {
    thread_pool_t *thread_pool = malloc(sizeof(thread_pool_t));
    if (thread_pool == NULL) {
        exit(EXIT_FAILURE);
    }

    thread_pool->queue = queue_create();

    if (pthread_mutex_init(&thread_pool->queue_mutex, NULL)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&thread_pool->queue_nonempty, NULL)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_key_create(&thread_pool->key_actor_id, NULL)) {
        exit(EXIT_FAILURE);
    }

    thread_pool->threads = malloc(sizeof(pthread_t) * (POOL_SIZE + 1));
    if (thread_pool->threads == NULL) {
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < POOL_SIZE; i++) {
        if (pthread_create(&thread_pool->threads[i], NULL, thread_function, NULL)) {
            exit(EXIT_FAILURE);
        }
    }
    pthread_attr_t daemon_attr;
    if (pthread_attr_init(&daemon_attr)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_attr_setdetachstate(&daemon_attr, PTHREAD_CREATE_DETACHED)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_create(&thread_pool->threads[POOL_SIZE], &daemon_attr,
                       thread_signal_handler_function, NULL)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_attr_destroy(&daemon_attr)) {
        exit(EXIT_FAILURE);
    }

    return thread_pool;
}

int thread_pool_join(thread_pool_t *thread_pool) {
    for (size_t i = 0; i < POOL_SIZE; i++) {
        void *ret_val;
        if (pthread_join(thread_pool->threads[i], &ret_val)) {
            exit(EXIT_FAILURE);
        }
    }

    return 0;
}

void thread_pool_destroy(thread_pool_t *thread_pool) {
    queue_destroy(thread_pool->queue);

    if (pthread_mutex_destroy(&thread_pool->queue_mutex)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_destroy(&thread_pool->queue_nonempty)) {
        exit(EXIT_FAILURE);
    }
    if (pthread_key_delete(thread_pool->key_actor_id)) {
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
    int err = sigaction(SIGINT, &actor_system.sigaction, NULL);
    if (err != 0) {
        //TODO: error handling
        return err;
    }
    else {
        actor_system.thread_pool = thread_pool_create();
        for (size_t i = 0; i < CAST_LIMIT; i++) {
            actor_system.actors[i] = NULL;
        }
        actor_system.spawned_actors = 0;
        actor_system.spawning_allowed = true;
        actor_system.dead_empty_actors = 0;
        if (pthread_mutex_init(&actor_system.actors_mutex, NULL)) {
            exit(EXIT_FAILURE);
        }

        return 0;
    }
}

actor_id_t actor_system_spawn_actor(role_t *role) {
    if (pthread_mutex_lock(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }

    if (!actor_system.spawning_allowed || CAST_LIMIT <= actor_system.spawned_actors) {
        if (pthread_mutex_unlock(&actor_system.actors_mutex)) {
            exit(EXIT_FAILURE);
        }

        return -1;
    }
    else {
        actor_id_t actor_id = actor_system.spawned_actors;
        actor_system.actors[actor_id] = actor_create(actor_id, role);
        actor_system.spawned_actors++;

        if (pthread_mutex_unlock(&actor_system.actors_mutex)) {
            exit(EXIT_FAILURE);
        }

        return actor_id;
    }
}

bool actor_system_legal_actor_id(actor_id_t actor) {
    if (pthread_mutex_lock(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }

    bool res = 0 <= actor && actor < CAST_LIMIT && actor_system.actors[actor] != NULL;

    if (pthread_mutex_unlock(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }

    return res;
}

void actor_system_dispose() {
    thread_pool_destroy(actor_system.thread_pool);
    for (size_t i = 0; i < actor_system.spawned_actors; i++) {
        actor_destroy(actor_system.actors[i]);
    }
    actor_system.spawned_actors = 0;
    actor_system.dead_empty_actors = 0;
    if (pthread_mutex_destroy(&actor_system.actors_mutex)) {
        exit(EXIT_FAILURE);
    }
}

actor_id_t actor_id_self() {
    actor_id_t *actor_id = pthread_getspecific(
            actor_system.thread_pool->key_actor_id);

    return *actor_id;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    int res = actor_system_init();
    if (res) {
        return res;
    }
    else {
        *actor = actor_system_spawn_actor(role);

        return *actor < 0 ? -1 : 0;
    }
}

void actor_system_join(actor_id_t actor) {
    if (!actor_system_legal_actor_id(actor)) {
        //TODO: error handling
    }
    else {
        thread_pool_join(actor_system.thread_pool);
        actor_system_dispose();
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

        if (pthread_mutex_lock(actor_mutex)) {
            exit(EXIT_FAILURE);
        }

        if (!actor_system.actors[actor]->alive) {
            if (pthread_mutex_unlock(actor_mutex)) {
                exit(EXIT_FAILURE);
            }

            return -1;
        }
        else {
            while (buffer_full(actor_buffer)) {
                if (pthread_cond_wait(actor_cond, actor_mutex)) {
                    exit(EXIT_FAILURE);
                }
            }

            bool schedule_actor = buffer_empty(actor_buffer);
            buffer_push(actor_buffer, message);

            if (pthread_mutex_unlock(actor_mutex)) {
                exit(EXIT_FAILURE);
            }

            if (schedule_actor) {
                actor_schedule_for_execution(actor);
            }

            return 0;
        }
    }
}
