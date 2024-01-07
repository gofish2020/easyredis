package list

import (
	"errors"
)

type Consumer func(i int, val interface{}) bool

type Expected func(actual interface{}) bool

// 双向链表,实现增Add/删Del/改Modify/查 Get

type LinkedList struct {
	first *node
	last  *node

	size int
}

type node struct {
	pre  *node
	next *node
	val  any
}

func newNode(val any) *node {

	return &node{val: val}
}

// Add push new node to the tail
func (l *LinkedList) Add(val interface{}) {
	n := newNode(val)

	if l.last == nil { // 空链表
		l.first = n
		l.last = n
	} else {
		n.pre = l.last
		l.last.next = n
		l.last = n
	}
	l.size++
}

func (l *LinkedList) find(index int) *node {
	// 要找的节点在链表的前半部分
	if index < l.Len()/2 {
		n := l.first
		for i := 0; i < index; i++ {
			n = n.next
		}
		return n
	}
	// 要找的节点在链表的后半部分
	n := l.last
	for i := l.Len() - 1; i > index; i-- {
		n = n.pre
	}
	return n
}

// 获取指定索引节点的值
func (l *LinkedList) Get(index int) (any, error) {
	if index < 0 || index >= l.size {
		return nil, errors.New("out of range")
	}
	n := l.find(index)
	return n.val, nil

}

// 修改指定节点的值
func (l *LinkedList) Modify(index int, val any) error {
	if index < 0 || index >= l.size {
		return errors.New("out of range")
	}

	n := l.find(index)
	n.val = val
	return nil
}

func (l *LinkedList) delNode(n *node) {
	// n 的前驱节点
	pre := n.pre
	// n 的后驱节点
	next := n.next

	if pre != nil {
		pre.next = next
	} else { // 说明n就是第一个节点
		l.first = next
	}

	if next != nil {
		next.pre = pre
	} else { // 说明n就是最后一个节点
		l.last = pre
	}

	// for gc
	n.pre = nil
	n.next = nil

	l.size--
}

// 删除指定节点
func (l *LinkedList) Del(index int) (any, error) {
	if index < 0 || index >= l.size {
		return nil, errors.New("out of range")
	}
	n := l.find(index)
	l.delNode(n)
	return n.val, nil
}

// 删除最后一个节点
func (l *LinkedList) DelLast() (any, error) {
	if l.Len() == 0 { // do nothing
		return nil, nil
	}
	return l.Del(l.Len() - 1)
}

// 遍历链表中的元素
func (l *LinkedList) ForEach(consumer Consumer) {
	i := 0
	for n := l.first; n != nil; n = n.next {
		if !consumer(i, n.val) {
			break
		}
	}
}

// 判断是否包含指定值
func (l *LinkedList) Contain(expect Expected) bool {
	result := false
	l.ForEach(func(index int, val interface{}) bool {
		if expect(val) {
			result = true
			return false
		}
		return true
	})
	return result
}

// 删除链表中的指定值（所有）
func (l *LinkedList) DelAllByVal(expected Expected) int {

	removed := 0
	for n := l.first; n != nil; {
		next := n.next
		if expected(n.val) {
			l.delNode(n)
			removed++
		}
		n = next
	}
	return removed
}

// 链表的长度
func (l *LinkedList) Len() int {
	return l.size
}

// 构建新链表
func NewLinkedList() *LinkedList {
	l := &LinkedList{}
	return l
}
