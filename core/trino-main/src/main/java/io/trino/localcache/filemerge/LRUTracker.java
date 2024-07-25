/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.localcache.filemerge;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

public class LRUTracker<E>
{
    class Node
    {
        E elem;
        Node prev;
        Node next;
    }

    private final Map<E, Node> cache = new HashMap<>();
    private final Node head;
    private final Node tail;

    private void addNode(Node node)
    {
        node.prev = head;
        node.next = head.next;

        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(Node node)
    {
        Node prev = node.prev;
        Node next = node.next;

        prev.next = next;
        next.prev = prev;
    }

    private void moveToHead(Node node)
    {
        removeNode(node);
        addNode(node);
    }

    private Node popHead()
    {
        Node res = head.next;
        removeNode(res);
        return res;
    }

    private Node popTail()
    {
        Node res = tail.prev;
        removeNode(res);
        return res;
    }

    public LRUTracker()
    {
        head = new Node();
        head.prev = null;

        tail = new Node();
        tail.next = null;

        head.next = tail;
        tail.prev = head;
    }

    public synchronized E get(E elem)
    {
        Node node = cache.get(elem);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.elem;
    }

    public synchronized void put(E elem)
    {
        if (elem == null) {
            throw new NullPointerException();
        }
        Node node = cache.get(elem);

        if (node == null) {
            node = new Node();
            node.elem = elem;
            cache.put(elem, node);
            addNode(node);
        }
        else {
            moveToHead(node);
        }
    }

    public synchronized void remove(E elem)
    {
        if (elem == null) {
            return;
        }
        Node node = cache.remove(elem);
        if (node != null) {
            removeNode(node);
        }
    }

    // Removes and returns the Least Recently Used element, or returns null if empty.
    public synchronized E pollLeast()
    {
        if (tail.prev != head) {
            Node node = popTail();
            cache.remove(node.elem);
            return node.elem;
        }
        else {
            return null;
        }
    }

    // Retrieves, but does not remove, the Least Recently Used element, or returns null if empty.
    public synchronized E peekLeast()
    {
        Node node = tail.prev;
        if (node != head) {
            return node.elem;
        }
        else {
            return null;
        }
    }

    // Removes and returns the Most Recently Used element, or returns null if empty.
    public synchronized E pollMost()
    {
        if (head.next != tail) {
            Node node = popHead();
            cache.remove(node.elem);
            return node.elem;
        }
        else {
            return null;
        }
    }

    // Retrieves, but does not remove, the Most Recently Used element, or returns null if empty.
    public synchronized E peekMost()
    {
        Node node = head.next;
        if (node != tail) {
            return node.elem;
        }
        else {
            return null;
        }
    }

    public synchronized boolean contains(E elem)
    {
        return cache.containsKey(elem);
    }

    public synchronized boolean isEmpty()
    {
        return cache.isEmpty();
    }

    public synchronized int size()
    {
        return cache.size();
    }

    @VisibleForTesting
    synchronized int listSize()
    {
        int count = 0;
        Node node = head;
        while (node.next != tail) {
            node = node.next;
            count++;
        }
        return count;
    }

    @VisibleForTesting
    synchronized boolean containsAll()
    {
        Node node = head;
        while (node.next != tail) {
            node = node.next;
            Node mapNode = cache.get(node.elem);
            if (mapNode == null) {
                return false;
            }
            if (mapNode.elem != node.elem) {
                return false;
            }
        }
        return true;
    }
}
