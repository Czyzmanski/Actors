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

typedef struct node node_t;

struct node {
    actor_id_t actor_id;
    node_t *next;
};

node_t *node_create(actor_id_t actor_id, node_t *next) {
    node_t *node = malloc(sizeof(node_t));
    assert(node != NULL);
    node->actor_id = actor_id;
    node->next = next;

    return node;
}

void node_destroy(node_t *node) {
    assert(node != NULL);
    free(node);
}

typedef struct queue {
    node_t *first;
    node_t *last;
} queue_t;


queue_t *queue_create() {
    queue_t *queue = malloc(sizeof(queue_t));
    assert(queue != NULL);
    queue->first = NULL;
    queue->last = NULL;

    return queue;
}

bool queue_empty(queue_t *queue) {
    assert(queue != NULL);
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
    assert(queue != NULL);
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

#define BUFFER_SIZE 2048

typedef struct buffer {
    size_t first_pos;
    size_t last_pos;
    size_t size;
    message_t messages[BUFFER_SIZE];
} buffer_t;

buffer_t *buffer_create() {
    buffer_t *buffer = malloc(sizeof(buffer_t));
    assert(buffer != NULL);
    buffer->first_pos = 0;
    buffer->last_pos = 0;
    buffer->size = 0;

    return buffer;
}

bool buffer_empty(buffer_t *buffer) {
    assert(buffer != NULL);
    return buffer->size == 0;
}

bool buffer_full(buffer_t *buffer) {
    assert(buffer != NULL);
    return buffer->size == BUFFER_SIZE;
}

void buffer_destroy(buffer_t *buffer) {
    assert(buffer != NULL);
    free(buffer);
}

actor_id_t actor_id_self() {

}

int actor_system_create(actor_id_t *actor, role_t *const role) {

}

void actor_system_join(actor_id_t actor) {

}

int send_message(actor_id_t actor, message_t message) {

}

