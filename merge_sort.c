#include <stdlib.h>
#include <string.h>
#include "merge_sort.h"

/**
 * @brief Sort and merge the two lists _a_ and _b_.
 */
llist_t *sort_n_merge(llist_t *a, llist_t *b)
{
    llist_t *_list = list_new();
    node_t *current = _list->head;
    llist_t *small;
    while (a->size && b->size) {
        /* Choose the linked list whose data of first node is small. */
        int cmp = strcmp((char*)(a->head->next->data), (char*)(b->head->next->data));
        small = (llist_t *)((intptr_t) a * (cmp <= 0) + (intptr_t) b * (cmp > 0));
        current->next = small->head->next;
        current = current->next;
        small->head->next = current->next;
        --small->size;
        ++_list->size;
        current->next = _list->tail;
    }
    /* Append the remaining nodes */
    llist_t *remaining = (llist_t *) ((intptr_t) a * (a->size > 0) +
                                      (intptr_t) b * (b->size > 0));
    node_t *tail_prev = list_get(remaining, remaining->size - 1);
    current->next = remaining->head->next;
    _list->size += remaining->size;
    tail_prev->next = _list->tail;
    remaining->head->next = remaining->tail;
    //list_free_empty(a);
    //list_free_empty(b);
    list_free_nodes(a);
    list_free_nodes(b);
    return _list;
}

/**
 * @brief Split the list and merge later.
 */
llist_t *split_n_merge(llist_t *list)
{
    if (list->size < 2)
        return list;
    int mid = list->size / 2;
    llist_t *left = list;
    llist_t *right = list_new();
    node_t *left_tail_prev = list_get(list, mid - 1);
    node_t *right_tail_prev = list_get(list, list->size - 1);
    right->head->next = left_tail_prev->next;
    left_tail_prev->next = left->tail;
    right_tail_prev->next = right->tail;
    right->size = list->size - mid;
    left->size = mid;
    return sort_n_merge(split_n_merge(left), split_n_merge(right));
}

llist_t *merge_sort(llist_t *list)
{
    return split_n_merge(list);
}
