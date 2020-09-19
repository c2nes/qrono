package net.qrono.server.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

public class LinkedNodeListTest {

  @Test
  public void testAdd_single() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));

    assertListsEquals(List.of("Hello 1"), list);
  }

  @Test
  public void testAdd_multiple() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.add(value("Hello 2"));
    list.add(value("Hello 3"));

    assertListsEquals(List.of("Hello 1", "Hello 2", "Hello 3"), list);
  }

  @Test
  public void testInsert_first() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 2"));
    list.add(value("Hello 3"));
    list.add(0, value("Hello 1"));

    assertListsEquals(List.of("Hello 1", "Hello 2", "Hello 3"), list);
  }

  @Test
  public void testInsert_last() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.add(value("Hello 2"));
    list.add(2, value("Hello 3"));

    assertListsEquals(List.of("Hello 1", "Hello 2", "Hello 3"), list);
  }

  @Test
  public void testRemove_firstIndex() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.add(value("Hello 2"));
    list.add(value("Hello 3"));
    list.remove(0);

    assertListsEquals(List.of("Hello 2", "Hello 3"), list);
  }

  @Test
  public void testRemove_middleIndex() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.add(value("Hello 2"));
    list.add(value("Hello 3"));
    list.remove(1);

    assertListsEquals(List.of("Hello 1", "Hello 3"), list);
  }

  @Test
  public void testRemove_lastIndex() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.add(value("Hello 2"));
    list.add(value("Hello 3"));
    list.remove(2);

    assertListsEquals(List.of("Hello 1", "Hello 2"), list);
  }

  @Test
  public void testRemove_onlyIndex() {
    var list = new LinkedNodeList<Value<String>>();
    list.add(value("Hello 1"));
    list.remove(0);

    assertListsEquals(List.of(), list);
  }

  @Test
  public void testRemove_firstValue() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    list.addAll(List.of(val1, val2, val3));
    list.remove(val1);

    assertListsEquals(List.of("Hello 2", "Hello 3"), list);
  }

  @Test
  public void testRemove_middleValue() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    list.addAll(List.of(val1, val2, val3));
    list.remove(val2);

    assertListsEquals(List.of("Hello 1", "Hello 3"), list);
  }

  @Test
  public void testRemove_lastValue() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    list.addAll(List.of(val1, val2, val3));
    list.remove(val3);

    assertListsEquals(List.of("Hello 1", "Hello 2"), list);
  }

  @Test
  public void testRemove_onlyValue() {
    var list = new LinkedNodeList<Value<String>>();
    var val = value("Hello 1");
    list.add(val);
    list.remove(val);

    assertListsEquals(List.of(), list);
  }

  @Test
  public void testSet_first() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    var val4 = value("Hello 4");
    list.addAll(List.of(val1, val2, val3));
    list.set(0, val4);

    assertListsEquals(List.of("Hello 4", "Hello 2", "Hello 3"), list);
  }

  @Test
  public void testSet_middle() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    var val4 = value("Hello 4");
    list.addAll(List.of(val1, val2, val3));
    list.set(1, val4);

    assertListsEquals(List.of("Hello 1", "Hello 4", "Hello 3"), list);
  }

  @Test
  public void testSet_last() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    var val3 = value("Hello 3");
    var val4 = value("Hello 4");
    list.addAll(List.of(val1, val2, val3));
    list.set(2, val4);

    assertListsEquals(List.of("Hello 1", "Hello 2", "Hello 4"), list);
  }

  @Test
  public void testSet_only() {
    var list = new LinkedNodeList<Value<String>>();
    var val1 = value("Hello 1");
    var val2 = value("Hello 2");
    list.add(val1);
    list.set(0, val2);

    assertListsEquals(List.of("Hello 2"), list);
  }

  private <E> void assertListsEquals(List<E> expected, LinkedNodeList<Value<E>> actual) {
    assertEquals(expected.size(), actual.size());

    var forwards = new ArrayList<E>();
    var iter = actual.listIterator();
    while (iter.hasNext()) {
      forwards.add(iter.next().get());
    }

    var backwards = new ArrayList<E>();
    iter = actual.listIterator(actual.size());
    while (iter.hasPrevious()) {
      backwards.add(iter.previous().get());
    }

    assertEquals(expected, forwards);
    assertEquals(Lists.reverse(expected), backwards);
  }

  static <E> Value<E> value(E value) {
    return new Value<>(value);
  }

  static class Value<E> extends LinkedNode<Value<E>> {
    private final E value;

    Value(E value) {
      this.value = value;
    }

    E get() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Value<?> value1 = (Value<?>) o;
      return Objects.equals(value, value1.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }
}