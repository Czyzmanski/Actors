#include <stdio.h>
#include <stdlib.h>

#include "cacti.h"

#define MESSAGES_TYPES 4
#define MSG_INIT_REQUEST 1
#define MSG_INIT 2
#define MSG_FINISH 3

typedef unsigned long long ull_t;

size_t n;

typedef struct init_data {
    size_t n;
    ull_t fact;
    role_t *role;
} init_data_t;

typedef struct fact_comp {
    size_t n;
    ull_t fact;
    actor_id_t id_self;
    actor_id_t *parent;
    role_t *role;
    init_data_t on_init_request_data;
} fact_comp_t;


void on_hello(void **stateptr, size_t nbytes __attribute__((unused)), void *data) {
    *stateptr = malloc(sizeof(fact_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->id_self = actor_id_self();
    fact_comp->parent = data;

    if (fact_comp->parent != NULL) {
        message_t init_request;
        init_request.message_type = MSG_INIT_REQUEST;
        init_request.nbytes = sizeof(actor_id_t);
        init_request.data = &fact_comp->id_self;

        int err;
        if ((err = send_message(*fact_comp->parent, init_request))) {
            //TODO: error handling
        }
    }
}

void on_init_request(void **stateptr,
                     size_t nbytes __attribute__((unused)), void *data) {
    actor_id_t *requester = data;

    fact_comp_t *fact_comp = *stateptr;
    fact_comp->on_init_request_data.n = fact_comp->n + 1;
    fact_comp->on_init_request_data.fact = fact_comp->fact * (fact_comp->n + 1);
    fact_comp->on_init_request_data.role = fact_comp->role;

    message_t init;
    init.message_type = MSG_INIT;
    init.nbytes = sizeof(init_data_t);
    init.data = &fact_comp->on_init_request_data;

    int err;
    if ((err = send_message(*requester, init))) {
        //TODO: error handling
    }
}

void on_init(void **stateptr, size_t nbytes __attribute__((unused)), void *data) {
    init_data_t *init_data = data;
    fact_comp_t *fact_comp = *stateptr;
    fact_comp->n = init_data->n;
    fact_comp->fact = init_data->fact;
    fact_comp->role = init_data->role;
    fact_comp->id_self = actor_id_self();

    int err;
    if (fact_comp->n == n) {
        printf("%llu", fact_comp->fact);

        message_t finish;
        finish.message_type = MSG_FINISH;
        finish.nbytes = 0;
        finish.data = NULL;

        if ((err = send_message(fact_comp->id_self, finish))) {
            //TODO: error handling
        }
    }
    else {
        message_t spawn;
        spawn.message_type = MSG_SPAWN;
        spawn.nbytes = sizeof(role_t);
        spawn.data = fact_comp->role;

        if ((err = send_message(actor_id_self(), spawn))) {
            //TODO: error handling
        }
    }
}

void on_finish(void **stateptr, size_t nbytes __attribute__((unused)),
               void *data __attribute__((unused))) {
    fact_comp_t *fact_comp = *stateptr;
    int err;

    if (fact_comp->n > 0) {
        message_t finish;
        finish.message_type = MSG_FINISH;
        finish.nbytes = 0;
        finish.data = NULL;

        if ((err = send_message(*fact_comp->parent, finish))) {
            //TODO: error handling
        }
    }

    message_t go_die;
    go_die.message_type = MSG_GODIE;
    go_die.nbytes = 0;
    go_die.data = NULL;

    free(fact_comp);
    *stateptr = NULL;

    if ((err = send_message(actor_id_self(), go_die))) {
        //TODO: error handling
    }
}

int main() {
    scanf("%zd", &n);

    act_t acts[] = {on_hello, on_init_request, on_init, on_finish};
    role_t role;
    role.nprompts = MESSAGES_TYPES;
    role.prompts = acts;

    actor_id_t first_actor;
    int err;
    if ((err = actor_system_create(&first_actor, &role))) {
        //TODO: error handling
        return err;
    }

    init_data_t init_data;
    init_data.n = 0;
    init_data.fact = 1;
    init_data.role = &role;

    message_t start_computation;
    start_computation.message_type = MSG_INIT;
    start_computation.nbytes = sizeof(init_data_t);
    start_computation.data = &init_data;

    if ((err = send_message(first_actor, start_computation))) {
        //TODO: error handling
        return err;
    }
    actor_system_join(first_actor);

    return 0;
}
