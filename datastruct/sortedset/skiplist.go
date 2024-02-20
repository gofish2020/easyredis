package sortedset

import (
	"math/bits"
	"math/rand"
	"time"
)

/*
跳表：增删改查
*/

const (
	defaultMaxLevel = 16
)

// 要存储的值
type Pair struct {
	Member string
	Score  float64
}

type Level struct {
	forward *node
	span    int64
}

// 单个节点（包括：值和指针）
type node struct {
	Pair              // 值
	backward *node    // 后向指针(倒序遍历，从尾部到头部)
	levels   []*Level // 前向指针（多层）
}

// 创建新节点
func newNode(level int16, member string, score float64) *node {

	n := &node{
		Pair: Pair{
			Member: member,
			Score:  score,
		},
	}
	// 节点的层初始化
	n.levels = make([]*Level, level)
	for i := range n.levels {
		n.levels[i] = &Level{}
	}
	return n
}

// 额外的数据结构，用来对链表进行管理
type skiplist struct {
	// 头节点
	header *node
	// 尾节点
	tailer   *node
	length   int64 // 节点个数
	maxLevel int16 // 跳表的最高层高（虚拟节点不算）
}

func newSkipList() *skiplist {
	skl := &skiplist{}

	skl.header = newNode(defaultMaxLevel, "", 0) // 虚拟节点
	skl.maxLevel = 1

	return skl
}

// 随机层高
func randomLevel() int16 {
	total := uint64(1)<<defaultMaxLevel - 1 // 0x 00 00 00 00 00 00 FF FF
	rand.Seed(int64(time.Now().Nanosecond()))
	k := rand.Uint64()%total + 1                      // k = [0,0xFFFE] + 1 = [1,0xFFFF]
	return defaultMaxLevel - int16(bits.Len64(k)) + 1 //16 - 计算表示数字k需要多少位[1,16] + 1 = [0,15] + 1 = [1,16]
}

// 增
func (s *skiplist) insert(member string, score float64) *node {

	beforeNode := make([]*node, defaultMaxLevel)      // 每一层的前驱节点
	beforeNodeOrder := make([]int64, defaultMaxLevel) // 每一层的前驱节点排序编号

	node := s.header
	// i从最高层遍历，通过遍历，在每一层的前驱节点全部保存在 beforeNode中，节点的编号保存在beforeNodeOrder
	for i := s.maxLevel - 1; i >= 0; i-- {

		// 节点node的编号
		if i == s.maxLevel-1 {
			beforeNodeOrder[i] = 0
		} else {
			beforeNodeOrder[i] = beforeNodeOrder[i+1]
		}
		// 节点node在当前i层的forward不为空
		for node.levels[i].forward != nil &&
			// node在层levels[i]的forward节点，分值 < score 或者 分值相同但是成员 < member，说明forward指向的节点，作为下一个前驱节点
			(node.levels[i].forward.Score < score || (node.levels[i].forward.Score == score && node.levels[i].forward.Member < member)) {

			beforeNodeOrder[i] += int64(node.levels[i].span) // 更新节点node的编号
			node = node.levels[i].forward                    // 更新当前节点node
		}
		beforeNode[i] = node
	}

	// 新节点层高
	newLevel := randomLevel()
	// 如果新层高比当前已经存在的层高s.maxLevel都要高，说明还缺少了 newLevel - s.maxLevel范围的前驱节点
	if newLevel > s.maxLevel {
		for i := s.maxLevel; i < newLevel; i++ {
			// beforeNode[i] 表示在i层的前驱节点
			beforeNode[i] = s.header
			beforeNodeOrder[i] = 0
			beforeNode[i].levels[i].forward = nil
			beforeNode[i].levels[i].span = s.length
		}

		// 更新最大层高
		s.maxLevel = newLevel
	}

	node = newNode(newLevel, member, score)

	// 将节点插入到多层链表中，仅仅对[0,newLevel）范围进行节点拼接
	for i := int16(0); i < newLevel; i++ {
		//也就是在每一层插入节点
		node.levels[i].forward = beforeNode[i].levels[i].forward
		beforeNode[i].levels[i].forward = node

		// 更新本层节点跨度
		node.levels[i].span = beforeNode[i].levels[i].span - (beforeNodeOrder[0] - beforeNodeOrder[i])
		beforeNode[i].levels[i].span = beforeNodeOrder[0] - beforeNodeOrder[i] + 1
	}

	// 如果新节点的高度很低，比最高低很多
	for i := newLevel; i < s.maxLevel; i++ {
		beforeNode[i].levels[i].span++ // 超过的节点的跨度默认+1
	}

	// 修改第0层的 backward指向
	if beforeNode[0] == s.header {
		node.backward = nil
	} else {
		node.backward = beforeNode[0]
	}

	if node.levels[0].forward != nil {
		node.levels[0].forward.backward = node
	} else { // 说明node是最后一个节点
		s.tailer = node
	}
	// 因为新增，数量+1
	s.length++
	return node
}

// 删

func (s *skiplist) remove(member string, score float64) bool {

	beforeNode := make([]*node, defaultMaxLevel)

	node := s.header
	// 查找被删除值的【各个层的前驱节点】
	for i := s.maxLevel - 1; i >= 0; i-- {

		// 当前i层前驱节点
		for node.levels[i].forward != nil &&
			(node.levels[i].forward.Score < score ||
				(node.levels[i].forward.Score == score && node.levels[i].forward.Member < member)) {
			node = node.levels[i].forward
		}
		beforeNode[i] = node
	}

	// 被删除的节点
	node = node.levels[0].forward
	// 判断节点值是否正确
	if node != nil && node.Score == score && node.Member == member {
		s.removeNode(node, beforeNode)
		return true
	}
	return false
}

// 通过前驱节点，删除当前node节点
func (s *skiplist) removeNode(node *node, beforeNode []*node) {

	// 从每一层中删除节点node
	for i := int16(0); i < s.maxLevel; i++ {

		// 在第i层的前驱节点 beforeNode[i] 和node有直接指向关系
		if beforeNode[i].levels[i].forward == node {
			// 断开连接
			beforeNode[i].levels[i].span += node.levels[i].span - 1
			beforeNode[i].levels[i].forward = node.levels[i].forward

		} else { // 和node没有指向关系
			beforeNode[i].levels[i].span--
		}
	}

	// 修改backward
	if node.levels[0].forward != nil {
		node.levels[0].forward.backward = node.backward
	} else {
		s.tailer = node.backward // 说明node是最后一个节点
	}

	//重新计算最高层高
	for s.maxLevel > 1 && s.header.levels[s.maxLevel-1].forward == nil {
		s.maxLevel--
	}
	s.length--
}

// 查：0表示没有查到
func (s *skiplist) getRank(member string, score float64) int64 {
	var nodeOrder int64
	node := s.header
	for i := s.maxLevel - 1; i >= 0; i-- {
		// 查找当前层的前驱节点node
		for node.levels[i].forward != nil &&
			(node.levels[i].forward.Score < score ||
				(node.levels[i].forward.Score == score && node.levels[i].forward.Member <= member)) {

			nodeOrder += node.levels[i].span
			node = node.levels[i].forward
		}

		if node != s.header && node.Score == score && node.Member == member {
			return nodeOrder
		}
	}
	return 0
}

func (s *skiplist) getByRank(order int64) *node {

	var nodeOrder int64 = 0

	node := s.header
	for i := s.maxLevel - 1; i >= 0; i-- {
		// 每一层节点的排序
		for node.levels[i].forward != nil && (nodeOrder+node.levels[i].span <= order) {
			nodeOrder += node.levels[i].span
			node = node.levels[i].forward
		}
		if nodeOrder == order {
			return node
		}
	}

	return nil
}

func (s *skiplist) hasInRange(min, max Border) bool {
	if min.isIntersected(max) { // min和max有交叉（表示min和max表示的范围无意义）
		return false
	}

	tail := s.tailer
	if tail == nil || !min.less(&tail.Pair) { // tail < min
		return false
	}

	head := s.header.levels[0].forward
	if head == nil || !max.greater(&head.Pair) { // head > max
		return false
	}
	return true
}

// 获取 min,max 范围内的第一个节点
func (s *skiplist) getFirstInRange(min Border, max Border) *node {

	// 判断min和max范围是否有效的范围
	if !s.hasInRange(min, max) {
		return nil
	}

	node := s.header
	// 两个for循环执行,让node无限趋近于min
	for i := s.maxLevel - 1; i >= 0; i-- {
		// 每一层的前驱节点
		for node.levels[i].forward != nil && !min.less(&node.levels[i].forward.Pair) {
			node = node.levels[i].forward
		}
	}

	// node.levels[0].forward  就找到范围内的第一个节点
	node = node.levels[0].forward
	if !max.greater(&node.Pair) {
		return nil
	}
	return node
}

// 获取 min,max 范围内的最后一个节点
func (s *skiplist) getLastInRange(min Border, max Border) *node {
	// 判断min和max范围是否有效的范围
	if !s.hasInRange(min, max) {
		return nil
	}
	node := s.header
	// 通过两层for循环，让node无限趋近于max
	for i := s.maxLevel - 1; i >= 0; i-- {
		// 每一层的前驱节点
		for node.levels[i].forward != nil && max.greater(&node.levels[i].forward.Pair) {
			node = node.levels[i].forward
		}
	}
	if !min.less(&node.Pair) {
		return nil
	}
	return node
}

// limit <= 0 表示将范围内的全部删除 limit > 0 表示只删除limit个
func (s *skiplist) RemoveRange(min, max Border, limit int) []*Pair {

	removed := make([]*Pair, 0)
	beforeNode := make([]*node, defaultMaxLevel)

	node := s.header
	for i := s.maxLevel - 1; i >= 0; i-- {
		for node.levels[i].forward != nil && !min.less(&node.levels[i].forward.Pair) {
			node = node.levels[i].forward
		}
		// 找最小边界每层的前驱节点
		beforeNode[i] = node
	}

	// node 是范围内第一个节点
	node = node.levels[0].forward

	// 扫描链表，进行删除
	for node != nil {
		if !max.greater(&node.Pair) { // 越界
			break
		}

		next := node.levels[0].forward
		removedPair := node.Pair
		removed = append(removed, &removedPair)
		// 删除node节点
		s.removeNode(node, beforeNode)
		if limit > 0 && len(removed) == limit { // 说明已经删除了limit个
			break
		}
		node = next
	}

	return removed
}

// [start,stop)
func (s *skiplist) RemoveRangeByRank(start, stop int64) []*Pair {

	removed := make([]*Pair, 0)
	var nodeOrder int64 = 0 // 节点编号
	beforeNode := make([]*node, defaultMaxLevel)
	node := s.header

	for i := s.maxLevel - 1; i >= 0; i-- {

		for node.levels[i].forward != nil && (nodeOrder+node.levels[i].span < start) {

			nodeOrder += node.levels[i].span
			node = node.levels[i].forward
		}
		// 找最小边界的前驱节点
		beforeNode[i] = node
	}

	nodeOrder++                   // 节点编号
	node = node.levels[0].forward // 第一个要被删除的节点

	for node != nil && nodeOrder < stop { // 删除到边界，停止
		next := node.levels[0].forward
		removedPair := node.Pair
		removed = append(removed, &removedPair)
		s.removeNode(node, beforeNode)

		// 编号+1
		nodeOrder++
		// 节点移位
		node = next
	}

	return removed
}
