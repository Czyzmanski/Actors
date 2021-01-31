#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "cacti.h"

#ifndef MESSAGES_TYPES
#define MESSAGES_TYPES 4
#endif

#ifndef MSG_INIT_REQUEST
#define MSG_INIT_REQUEST 1
#endif

#ifndef MSG_INIT
#define MSG_INIT 2
#endif

#ifndef MSG_FINISH
#define MSG_FINISH 3
#endif

typedef unsigned long long ull_t;

size_t n;

typedef struct init_data {
    size_t n;
    ull_t fact;
    role_t *role_for_children;
} init_data_t;

typedef struct fact_comp {
    size_t n;
    ull_t fact;
    actor_id_t id_self;
    actor_id_t parent;
    role_t *role_for_children;
    init_data_t on_init_request_data;
} fact_comp_t;


void on_hello(void **stateptr, size_t nbytes __attribute__((unused)), void *data) {
    *stateptr = malloc(sizeof(fact_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->id_self = actor_id_self();
    fact_comp->parent = (actor_id_t) data;

    message_t init_request = {
            .message_type = MSG_INIT_REQUEST,
            .nbytes = sizeof(actor_id_t),
            .data = &fact_comp->id_self
    };

    int err;
    if ((err = send_message(fact_comp->parent, init_request))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

void on_hello_first_actor(void **stateptr, size_t nbytes __attribute__((unused)),
                          void *data __attribute__((unused))) {
    *stateptr = malloc(sizeof(fact_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->id_self = actor_id_self();
}

void on_init_request(void **stateptr,
                     size_t nbytes __attribute__((unused)), void *data) {
    actor_id_t *requester = data;

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->on_init_request_data.n = fact_comp->n + 1;
    fact_comp->on_init_request_data.fact = fact_comp->fact * (fact_comp->n + 1);
    fact_comp->on_init_request_data.role_for_children = fact_comp->role_for_children;

    message_t init = {
            .message_type = MSG_INIT,
            .nbytes = sizeof(init_data_t),
            .data = &fact_comp->on_init_request_data
    };

    int err;
    if ((err = send_message(*requester, init))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

void on_init(void **stateptr, size_t nbytes __attribute__((unused)), void *data) {
    init_data_t *init_data = data;

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->n = init_data->n;
    fact_comp->fact = init_data->fact;
    fact_comp->role_for_children = init_data->role_for_children;

    int err;
    if (fact_comp->n == n) {
        printf("%llu\n", fact_comp->fact);

        message_t finish = {
                .message_type = MSG_FINISH,
                .nbytes = 0,
                .data = NULL
        };

        if ((err = send_message(fact_comp->id_self, finish))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }
    else {
        message_t spawn = {
                .message_type = MSG_SPAWN,
                .nbytes = sizeof(role_t),
                .data = fact_comp->role_for_children
        };

        if ((err = send_message(fact_comp->id_self, spawn))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }
}

void on_finish(void **stateptr, size_t nbytes __attribute__((unused)),
               void *data __attribute__((unused))) {
    fact_comp_t *fact_comp = *stateptr;

    int err;
    if (fact_comp->n > 0) {
        message_t finish = {
                .message_type = MSG_FINISH,
                .nbytes = 0,
                .data = NULL
        };

        if ((err = send_message(fact_comp->parent, finish))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }

    message_t go_die = {
            .message_type = MSG_GODIE,
            .nbytes = 0,
            .data = NULL
    };

    free(fact_comp);
    *stateptr = NULL;

    if ((err = send_message(actor_id_self(), go_die))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

int main() {
    scanf("%zd", &n);

    actor_id_t first_actor;
    act_t acts_for_first_actor[] = {on_hello_first_actor,
                                    on_init_request, on_init, on_finish};
    role_t role_for_first_actor = {
            .nprompts = MESSAGES_TYPES,
            .prompts = acts_for_first_actor
    };

    int err;
    if ((err = actor_system_create(&first_actor, &role_for_first_actor))) {
        fprintf(stderr, "Actor system creation failed: %d, %s\n",
                errno, strerror(errno));

        return err;
    }

    act_t acts_for_next_actors[] = {on_hello, on_init_request, on_init, on_finish};
    role_t role_for_next_actors = {
            .nprompts = MESSAGES_TYPES,
            .prompts = acts_for_next_actors
    };

    init_data_t init_data = {
            .n = 0,
            .fact = 1,
            .role_for_children = &role_for_next_actors
    };

    message_t start_computation = {
            .message_type = MSG_INIT,
            .nbytes = sizeof(init_data_t),
            .data = &init_data
    };

    if ((err = send_message(first_actor, start_computation))) {
        fprintf(stderr, "Sending message to the first actor failed: %d\n", err);

        return err;
    }

    actor_system_join(first_actor);

    return 0;
}
