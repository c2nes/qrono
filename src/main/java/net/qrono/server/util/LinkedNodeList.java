package net.qrono.server.util;

import com.google.common.base.Preconditions;
import java.util.AbstractSequentialList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Linked list implementation with next/prev pointers embedded within entries. Does not support
 * entries belonging to multiple lists. The behavior of methods accepting entry parameters is
 * undefined if those methods are called with entries contained in other {@code LinkedNodeLists}.
 *
 * @param <E> entry type, must extend {@link LinkedNode}
 */
public class LinkedNodeList<E extends LinkedNode<E>> extends AbstractSequentialList<E> {
  private int size = 0;
  private E head = null;
  private E tail = null;

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(Object o) {
    return (o instanceof LinkedNode) && contains((LinkedNode<E>) o);
  }

  /**
   * Returns true if the list contains the specified node. If the specified node is contained
   * in any list <em>other</em> than this list then the behavior of this method is undefined.
   */
  public boolean contains(LinkedNode<E> node) {
    // If the next pointer is null then the entry is the tail of this list, or
    // isn't in this list at all. This check could be implemented equivalently
    // using `prev` and `head`.
    return node.next != null || node == tail;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    return (o instanceof LinkedNode) && remove((LinkedNode<E>) o);
  }

  public boolean remove(LinkedNode<E> node) {
    Preconditions.checkNotNull(node);

    // If the next pointer is null then the entry is the tail of this list, or
    // isn't in this list at all. This check could be implemented equivalently
    // using `prev` and `head`.
    if (node.next == null && node != tail) {
      return false;
    }

    if (node.next != null) {
      node.next.prev = node.prev;
    } else {
      tail = node.prev;
    }

    if (node.prev != null) {
      node.prev.next = node.next;
    } else {
      head = node.next;
    }

    size--;
    node.next = null;
    node.prev = null;

    return true;
  }

  // Insert `e` before `next`
  private void insertBefore(E e, E next) {
    Preconditions.checkNotNull(e);

    // This check isn't foolproof -- we can't detect the case where a node
    // is the single element in another list.
    Preconditions.checkState(e.next == null && e.prev == null && e != tail,
        "node is already present in this or another list");

    e.next = next;

    if (next != null) {
      e.prev = next.prev;
      next.prev = e;
    } else {
      e.prev = tail;
      tail = e;
    }

    if (e.prev != null) {
      e.prev.next = e;
    } else {
      head = e;
    }

    size++;
  }

  @Override
  public boolean add(E e) {
    insertBefore(e, null);
    return true;
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    Preconditions.checkPositionIndex(index, size);

    // Fast path for positioning at end
    if (index == size) {
      return new Iter(null, size);
    }

    Iter it = new Iter(head, 0);
    while (it.nextIndex() < index) {
      it.next();
    }

    return it;
  }

  @Override
  public int size() {
    return size;
  }

  private class Iter implements ListIterator<E> {
    private E next;
    private E last = null;
    private int nextIdx;

    Iter(E next, int nextIdx) {
      this.next = next;
      this.nextIdx = nextIdx;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public E next() {
      var e = next;
      if (e == null) {
        throw new NoSuchElementException();
      }
      last = e;
      next = next.next;
      nextIdx++;
      return e;
    }

    @Override
    public boolean hasPrevious() {
      if (next == null) {
        return tail != null;
      }
      return next.prev != null;
    }

    @Override
    public E previous() {
      E e;

      if (next == null) {
        e = tail;
      } else {
        e = next.prev;
      }

      if (e == null) {
        throw new NoSuchElementException();
      }

      last = e;
      next = e;
      nextIdx--;

      return e;
    }

    @Override
    public int nextIndex() {
      return nextIdx;
    }

    @Override
    public int previousIndex() {
      return nextIdx - 1;
    }

    @Override
    public void remove() {
      if (last == null) {
        throw new IllegalStateException();
      }

      LinkedNodeList.this.remove(last);

      if (last == next) {
        // Last call was to previous()
        next = last.next;
      } else {
        // Last call was to next()
        nextIdx--;
      }

      last = null;
    }

    @Override
    public void set(E e) {
      if (last == null) {
        throw new IllegalStateException();
      }
      e.next = last.next;
      e.prev = last.prev;
      if (e.prev != null) {
        e.prev.next = e;
      } else {
        head = e;
      }
      if (e.next != null) {
        e.next.prev = e;
      } else {
        tail = e;
      }
      last.next = null;
      last.prev = null;
      last = e;
    }

    @Override
    public void add(E e) {
      insertBefore(e, next);
      nextIdx++;
      last = null;
    }
  }
}
