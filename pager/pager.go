package pager

import (
	"encoding/json"
)

type Pager struct {
	PageSize    int `json:"pageSize"`
	PageCount   int `json:"pageCount"`
	RecordCount int `json;"recordCount"`
	Page        int `json:"page"`
	Start       int `json:"start"`
}

type PagerQuery struct {
	Skip  int
	Limit int
}

func (p *Pager) String() string {
	j, _ := json.Marshal(p)
	return string(j)
}

func (p *Pager) GetPager(pageSize int, page int, recordCount int) {
	p.Start = 0
	p.PageSize = pageSize
	p.RecordCount = recordCount
	if page < 1 {
		p.Page = 0
	} else {
		p.Page = page - 1
	}
	if pageSize < 1 {
		p.PageCount = 1
	} else {
		if recordCount%pageSize == 0 {
			p.PageCount = recordCount / pageSize
		} else {
			p.PageCount = recordCount/pageSize + 1
		}
		p.Start = p.Page * pageSize
	}
}

func NewPager(pageSize int, recordCount int) *Pager {
	pageCount := 0
	if recordCount%pageSize == 0 {
		pageCount = recordCount / pageSize
	} else {
		pageCount = recordCount/pageSize + 1
	}
	return &Pager{
		PageSize:    pageSize,
		RecordCount: recordCount,
		Page:        0,
		Start:       0,
		PageCount:   pageCount,
	}
}

func (p *Pager) NextPager() bool {
	if p.Page >= (p.PageCount - 1) {
		return false
	}
	p.Page = p.Page + 1
	p.Start = p.Page * p.PageSize
	return true
}
