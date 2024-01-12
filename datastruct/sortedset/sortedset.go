package sortedset

import (
	"errors"

	"github.com/gofish2020/easyredis/tool/logger"
)

// score可以相同，member不能重复
type SortedSet struct {
	dict map[string]*Pair // 利用map基于member去重
	skl  *skiplist        // 利用skl进行排序
}

func NewSortedSet() *SortedSet {

	ss := SortedSet{}
	ss.dict = make(map[string]*Pair)
	ss.skl = newSkipList()
	return &ss
}

// bool 为true表示新增， false表示修改
func (s *SortedSet) Add(member string, score float64) bool {
	pair, ok := s.dict[member]

	s.dict[member] = &Pair{
		Member: member,
		Score:  score,
	}

	// 说明是重复添加
	if ok {
		// 分值不同
		if score != pair.Score {
			// 将原来的从跳表中删除
			s.skl.remove(pair.Member, pair.Score)
			// 插入新值
			s.skl.insert(member, score)
		}
		// 分值相同，do nothing...
		return false
	}
	// 新增
	s.skl.insert(member, score)
	return true
}

func (s *SortedSet) Len() int64 {
	return int64(len(s.dict))
}

func (s *SortedSet) Get(member string) (*Pair, bool) {
	pair, ok := s.dict[member]
	if !ok {
		return nil, false
	}
	return pair, true
}

func (s *SortedSet) Remove(member string) bool {

	pair, ok := s.dict[member]
	if ok {
		s.skl.remove(pair.Member, pair.Score)
		delete(s.dict, member)
		return true
	}

	return false
}

// 获取在链表中的排序索引号
func (s *SortedSet) GetRank(member string, desc bool) (rank int64) {
	pair, ok := s.dict[member]
	if !ok {
		return -1
	}
	r := s.skl.getRank(pair.Member, pair.Score)
	if desc {
		r = s.skl.length - r
	} else {
		r--
	}
	return r
}

// start / stop都是正数
// 进行范围查询,扫描有序链表[start,stop)索引范围的节点，desc表示按照正序还是倒序
func (s *SortedSet) ForEachByRank(start, stop int64, desc bool, consumer func(pair *Pair) bool) error {
	// 节点个数
	size := s.Len()

	// start不能越界
	if start < 0 || start >= size {
		return errors.New("start out of range")
	}
	// stop不能越界
	if start > stop || stop > size {
		return errors.New("stop is illegal or out of range")
	}

	// 肯定要先找到该范围内的第一个节点
	var node *node
	if desc { // 表示倒着遍历链表
		node = s.skl.tailer //start==0，表示链表的倒数第一个节点
		if start > 0 {
			// size-start 就是正向的排序编号
			node = s.skl.getByRank(size - start) // start表示从链表尾部向前的索引（倒数），start=0表示链表倒数第一个节点，start=1表示链表倒数第二个节点
		}

	} else { // 正序遍历链表
		// start==0 ,表示正向的第一个节点
		node = s.skl.header.levels[0].forward
		// 如果索引>0
		if start > 0 {
			// 从skl链表中找到该节点（skl内部是按照从1开始计数），start索引是从0
			node = s.skl.getByRank(start + 1) // 所以这里要+1
		}

	}

	// 找到第一个节点后，就按照链表的方式扫描链表

	count := stop - start // 需要扫面的节点个数

	for i := 0; i < int(count); i++ {

		if !consumer(&node.Pair) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.levels[0].forward
		}
	}

	return nil
}

// 扫描[start,stop)范围的节点，起始索引从0开始
func (s *SortedSet) RangeByRank(start, stop int64, desc bool) []*Pair {
	sliceSize := stop - start

	slice := make([]*Pair, sliceSize)
	i := 0
	err := s.ForEachByRank(start, stop, desc, func(pair *Pair) bool {
		slice[i] = &Pair{
			Member: pair.Member,
			Score:  pair.Score,
		}
		i++
		return true
	})

	if err != nil {
		logger.Error("RangeByRank err", err)
	}
	return slice
}

// // 统计满足条件的节点个数
// func (s *SortedSet) RangeCount(min, max Border) int64 {
// 	var i int64 = 0
// 	// 遍历整个链表[0,s.Len())
// 	s.ForEachByRank(0, s.Len(), false, func(pair *Pair) bool {

// 		gtMin := min.less(pair)
// 		if !gtMin { // pair < min ，不符合
// 			return true // 小于左边界，继续遍历
// 		}

// 		ltMax := max.greater(pair)
// 		if !ltMax { //  pair > max
// 			return false // 超过右边界，停止遍历
// 		}
// 		// min <= pair <= max
// 		i++
// 		return true
// 	})

// 	return i
// }

func (s *SortedSet) RangeCount(min, max Border) int64 {

	// 找到范围内的第一个节点
	var node *node
	node = s.skl.getFirstInRange(min, max)

	var i int64 = 0
	// 扫描链表
	for node != nil {
		gtMin := min.less(&node.Pair)
		ltMax := max.greater(&node.Pair)
		// 不在范围内，跳出
		if !gtMin || !ltMax {
			break
		}
		i++
		node = node.levels[0].forward
	}
	return i
}

// 扫描[min,max] 范围内的节点，从偏移min + offset位置开始，扫面 count 个元素
func (s *SortedSet) ForEach(min, max Border, offset int64, count int64, desc bool, consumer func(pair *Pair) bool) {

	var node *node

	// 查找边界节点
	if desc {
		node = s.skl.getLastInRange(min, max)
	} else {
		node = s.skl.getFirstInRange(min, max)
	}

	// 让node偏移offset
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.levels[0].forward
		}
		offset--
	}

	if node == nil {
		return
	}

	// 扫描limit个元素(count 可能是负数，表示不限制个数，一直扫描到边界位置)
	for i := int64(0); i < count || count < 0; i++ {
		if !consumer(&node.Pair) {
			break
		}

		if desc {
			node = node.backward
		} else {
			node = node.levels[0].forward
		}

		// 如果下一个为nil，跳出
		if node == nil {
			break
		}
		// 判断node值是否在范围内
		gtMin := min.less(&node.Pair)
		ltMax := max.greater(&node.Pair)
		// 不在范围内，跳出
		if !gtMin || !ltMax {
			break
		}
	}
}

func (s *SortedSet) Range(min Border, max Border, offset int64, count int64, desc bool) []*Pair {
	if count == 0 || offset < 0 {
		return make([]*Pair, 0)
	}
	slice := make([]*Pair, 0)
	s.ForEach(min, max, offset, count, desc, func(element *Pair) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// 删除范围[min,max]的元素，这里的Border表示是Score或者Member
func (s *SortedSet) RemoveRange(min Border, max Border) int64 {
	// 从链表中删除
	removed := s.skl.RemoveRange(min, max, 0)

	// 从map中删除
	for _, pair := range removed {
		delete(s.dict, pair.Member)
	}
	return int64(len(removed))
}

// 删除最小的值
func (s *SortedSet) PopMin(count int) []*Pair {
	// 获取范围内的最小节点
	first := s.skl.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	// 将最小值作为左边界
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false, // 包含
	}
	// 删除范围内的count个元素
	removed := s.skl.RemoveRange(border, scorePositiveInfBorder, count)
	for _, pair := range removed {
		delete(s.dict, pair.Member)
	}
	return removed
}

// 表示删除索引[start,stop)的节点
func (s *SortedSet) RemoveByRank(start int64, stop int64) int64 {

	// 跳表的位置编号从1开始 [start+1,stop+1)
	removed := s.skl.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(s.dict, element.Member)
	}
	return int64(len(removed))
}
