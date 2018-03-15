package server

import (
	catapi "github.com/elxirhealth/catalog/pkg/catalogapi"
)

// publicationReceipts is a min-heap of PublicationReceipt objects sorted ascending by ReceivedTime
type publicationReceipts []*catapi.PublicationReceipt

func (prs publicationReceipts) Len() int {
	return len(prs)
}

func (prs publicationReceipts) Less(i, j int) bool {
	return prs[i].ReceivedTime < prs[j].ReceivedTime
}

func (prs publicationReceipts) Swap(i, j int) {
	prs[i], prs[j] = prs[j], prs[i]
}

func (prs *publicationReceipts) Push(x interface{}) {
	*prs = append(*prs, x.(*catapi.PublicationReceipt))
}

func (prs *publicationReceipts) Pop() interface{} {
	old := *prs
	n := len(old)
	x := old[n-1]
	*prs = old[0 : n-1]
	return x
}

func (prs publicationReceipts) Peak() *catapi.PublicationReceipt {
	return prs[0]
}
