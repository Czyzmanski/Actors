#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "cacti.h"

#ifndef MESSAGES_TYPES
#define MESSAGES_TYPES 5
#endif

#ifndef MSG_INIT_REQUEST
#define MSG_INIT_REQUEST 1
#endif

#ifndef MSG_INIT
#define MSG_INIT 2
#endif

#ifndef MSG_COMPUTE
#define MSG_COMPUTE 3
#endif

#ifndef MSG_FINISH
#define MSG_FINISH 4
#endif

#define UNUSED(x) (void)(x)

size_t k, n;

typedef struct pair {
    int val;
    int time;
} pair_t;

typedef struct init_data {
    size_t col;
    int val;
    role_t *role_for_children;
} init_data_t;

typedef struct matrix_comp {
    size_t col;
    int val;
    actor_id_t id_self;
    actor_id_t parent;
    role_t *role_for_children;
    init_data_t on_init_request_data;
} matrix_comp_t;

pair_t **matrix;

typedef struct partial_sum {
    size_t row;
    long sum;
} partial_sum_t;

partial_sum_t *partial_sum;

void on_hello(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);

    *stateptr = malloc(sizeof(matrix_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    matrix_comp_t *matrix_comp = *stateptr;
    matrix_comp->id_self = actor_id_self();
    matrix_comp->parent = (actor_id_t) data;

    message_t init_request = {
            .message_type = MSG_INIT_REQUEST,
            .nbytes = sizeof(actor_id_t),
            .data = &matrix_comp->id_self
    };

    int err;
    if ((err = send_message(matrix_comp->parent, init_request))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

void on_hello_first_actor(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);
    UNUSED(data);

    *stateptr = malloc(sizeof(matrix_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    matrix_comp_t *matrix_comp = *stateptr;
    matrix_comp->id_self = actor_id_self();
}

void on_init_request(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);

    actor_id_t *requester = data;

    matrix_comp_t *matrix_comp = *stateptr;
    matrix_comp->on_init_request_data.col = matrix_comp->col - 1;
    matrix_comp->on_init_request_data.val = 0;
    matrix_comp->on_init_request_data.role_for_children = matrix_comp->role_for_children;

    message_t init = {
            .message_type = MSG_INIT,
            .nbytes = sizeof(init_data_t),
            .data = &matrix_comp->on_init_request_data
    };

    int err;
    if ((err = send_message(*requester, init))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

void on_init(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);

    init_data_t *init_data = data;
    matrix_comp_t *matrix_comp = *stateptr;
    matrix_comp->col = init_data->col;
    matrix_comp->val = init_data->val;
    matrix_comp->role_for_children = init_data->role_for_children;

    int err;
    if (matrix_comp->col == 0) {
        message_t compute = {
                .message_type = MSG_COMPUTE,
                .nbytes = sizeof(partial_sum_t),
                .data = &partial_sum[0]
        };

        if ((err = send_message(actor_id_self(), compute))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }
    else {
        message_t spawn = {
                .message_type = MSG_SPAWN,
                .nbytes = sizeof(role_t),
                .data = matrix_comp->role_for_children
        };

        if ((err = send_message(actor_id_self(), spawn))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }
}

void on_compute(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);

    matrix_comp_t *matrix_comp = *stateptr;
    partial_sum_t *partial_comp = data;
    size_t curr_row = partial_comp->row;
    pair_t matrix_cell = matrix[curr_row][matrix_comp->col];

    usleep(matrix_cell.time * 1000);

    matrix_comp->val = matrix_cell.val;
    partial_comp->sum += matrix_comp->val;

    int err;
    if (matrix_comp->col == n - 1) {
        printf("%ld\n", partial_comp->sum);
    }
    else {
        message_t compute = {
                .message_type = MSG_COMPUTE,
                .nbytes = sizeof(partial_sum_t),
                .data = partial_comp
        };

        if ((err = send_message(matrix_comp->parent, compute))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }

    if (matrix_comp->col == 0) {
        if (curr_row == k - 1) {
            message_t finish = {
                    .message_type = MSG_FINISH,
                    .nbytes = 0,
                    .data = NULL
            };

            if ((err = send_message(matrix_comp->id_self, finish))) {
                fprintf(stderr, "Sending message to an actor failed: %d\n", err);
            }
        }
        else {
            message_t compute = {
                    .message_type = MSG_COMPUTE,
                    .nbytes = sizeof(partial_sum_t),
                    .data = &partial_sum[curr_row + 1]
            };

            if ((err = send_message(matrix_comp->id_self, compute))) {
                fprintf(stderr, "Sending message to an actor failed: %d\n", err);
            }
        }
    }
}

void on_finish(void **stateptr, size_t nbytes, void *data) {
    UNUSED(nbytes);
    UNUSED(data);

    matrix_comp_t *matrix_comp = *stateptr;

    int err;
    if (matrix_comp->col < n - 1) {
        message_t finish = {
                .message_type = MSG_FINISH,
                .nbytes = 0,
                .data = NULL
        };

        if ((err = send_message(matrix_comp->parent, finish))) {
            fprintf(stderr, "Sending message to an actor failed: %d\n", err);
        }
    }

    message_t go_die = {
            .message_type = MSG_GODIE,
            .nbytes = 0,
            .data = NULL
    };

    free(matrix_comp);
    *stateptr = NULL;

    if ((err = send_message(actor_id_self(), go_die))) {
        fprintf(stderr, "Sending message to an actor failed: %d\n", err);
    }
}

int main() {
    scanf("%zd\n%zd", &k, &n);

    matrix = malloc(k * sizeof(pair_t *));
    if (matrix == NULL) {
        exit(EXIT_FAILURE);
    }

    partial_sum = malloc(k * sizeof(partial_sum_t));
    if (partial_sum == NULL) {
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < k; i++) {
        partial_sum[i].row = i;
        partial_sum[i].sum = 0;

        matrix[i] = malloc(n * sizeof(pair_t));
        if (matrix[i] == NULL) {
            exit(EXIT_FAILURE);
        }

        for (size_t j = 0; j < n; j++) {
            scanf("%d %d", &matrix[i][j].val, &matrix[i][j].time);
        }
    }

    actor_id_t first_actor;
    act_t acts_for_first_actor[] = {on_hello_first_actor, on_init_request,
                                    on_init, on_compute, on_finish};
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

    act_t acts_for_next_actors[] = {on_hello, on_init_request,
                                    on_init, on_compute, on_finish};
    role_t role_for_next_actors = {
            .nprompts = MESSAGES_TYPES,
            .prompts = acts_for_next_actors
    };

    init_data_t init_data = {
            .col = n - 1,
            .val = 0,
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

    for (size_t i = 0; i < k; i++) {
        free(matrix[i]);
    }
    free(matrix);
    free(partial_sum);

    return 0;
}
