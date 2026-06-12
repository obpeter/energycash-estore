package ebow

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"at.ourproject/energystore/model"
	"github.com/golang/glog"
)

var (
	connectionPool = NewPool(20)
)

func ClosePool() {
	connectionPool.Close()
}

//type ebowLogger struct {
//	level glog.Level
//}

//func (el ebowLogger) Infof(format string, args ...interface{}) {
//	glog.V(el.level).Infof(format, args...)
//}
//
//func (el ebowLogger) Warningf(format string, args ...interface{}) {
//	glog.Warningf(format, args...)
//}
//
//func (el ebowLogger) Errorf(format string, args ...interface{}) {
//	glog.Errorf(format, args...)
//}
//
//func (el ebowLogger) Debugf(format string, args ...interface{}) {
//	glog.V(el.level).Infof(format, args...)
//}

// Prevent multiple goroutines from accessing the same resource at the same time (forces turn taking)
type Turns struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func newTurns() *Turns {
	t := &Turns{
		m: make(map[string]*sync.Mutex),
	}
	return t
}

// Lock a resource by given name
func (t *Turns) lock(name string) func() {
	t.mu.Lock()
	l, ok := t.m[name]
	if !ok {
		l = &sync.Mutex{}
		t.m[name] = l
	}
	t.mu.Unlock()

	l.Lock()
	return l.Unlock
}

type IBowStorage interface {
	GetMeta(key string) (*model.RawSourceMeta, error)
	SetMeta(line *model.RawSourceMeta) error
	GetLineRange(bucket, prefix, key, until string) IRange
	SetLines(bucket string, line []*model.RawSourceLine) error
	GetLine(line *model.RawSourceLine) error
	ListBuckets() ([]string, error)
	GetTenant() string
	FindBuckets(start, end int64) ([]string, error)
}

type BowStorage struct {
	db       *DB
	dbObject *DbObject
	tenant   string
	ecId     string
	unlock   func()
}

var turns = newTurns()

//func OpenStorage(tenant, ecId string) (*BowStorage, error) {
//	t := strings.ToLower(tenant)
//	basePath := viper.GetString("persistence.path")
//	unlock := turns.lock(t)
//	db, err := ebow.Open(filepath.Join(fmt.Sprintf("%s/%s", basePath, t), ecId), ebow.SetLogger(ebowLogger{5}))
//	if err != nil {
//		unlock()
//		return nil, err
//	}
//	return &BowStorage{db, unlock}, nil
//}
//
//func (b *BowStorage) Close() {
//	_ = b.db.Close()
//	b.unlock()
//	return
//}

func OpenStorage(tenant, ecId string) (*BowStorage, error) {
	if len(tenant) > 8 {
		glog.Errorf("tenant %s is too long", tenant)
		return nil, fmt.Errorf("tenant is too long (%s)", tenant)
	}
	db := connectionPool.Get(tenant, ecId)
	if db == nil {
		return nil, errors.New("failed to connect to database")
	}
	return &BowStorage{db.Db, db, tenant, ecId, nil}, nil
}

func (b *BowStorage) Close() {
	connectionPool.Put(b.ecId, b.dbObject)
}

func (b *BowStorage) IsOpen() bool {
	return b.dbObject.Db != nil
}

func (b *BowStorage) GetTenant() string {
	return b.tenant
}

func (b *BowStorage) SetLines(bucket string, line []*model.RawSourceLine) error {
	return b.SetLinesRaw(bucket, line)
}

func (b *BowStorage) SetLinesG2(line []*model.RawSourceLine) error {
	return b.SetLinesRaw("rawdata", line)
}

func (b *BowStorage) SetLinesG3(line []*model.RawSourceLine) error {
	return b.SetLinesRaw("rawdata", line)
}

func (b *BowStorage) SetLinesRaw(bucket string, line []*model.RawSourceLine) error {
	i := make([]interface{}, len(line))
	for l := range line {
		i[l] = line[l]
	}
	return b.db.Bucket(bucket).PutBatch(i)
}

func (b *BowStorage) SetLine(line *model.RawSourceLine) error {
	return b.db.Bucket("rawdata").Put(line)
}

func (b *BowStorage) SetReport(line *model.EnergyReport) error {
	return b.db.Bucket("rawdata").Put(line)
}

func (b *BowStorage) GetReport(period string) (*model.EnergyReport, error) {
	var report model.EnergyReport = model.EnergyReport{}
	err := b.db.Bucket("rawdata").Get(period, &report)
	return &report, err
}

func (b *BowStorage) SetMeta(line *model.RawSourceMeta) error {

	return b.db.Bucket("metadata").Put(line)
}

func (b *BowStorage) GetMeta(key string) (*model.RawSourceMeta, error) {
	var rawMeta model.RawSourceMeta
	err := b.db.Bucket("metadata").Get(key, &rawMeta)
	return &rawMeta, err
}

func (b *BowStorage) GetLinePrefix(key string) *Iter {
	return b.db.Bucket("rawdata").Prefix(key)
}

func (b *BowStorage) GetLineRange(bucket, prefix, key, until string) IRange {
	return b.db.Bucket(bucket).Range(fmt.Sprintf("%s/%s", prefix, key), fmt.Sprintf("%s/%s", prefix, until))
}

func (b *BowStorage) GetLine(line *model.RawSourceLine) error {
	return b.db.Bucket("rawdata").Get(line.Id, line)
}
func (b *BowStorage) GetLineG2(line *model.RawSourceLine) error {
	return b.db.Bucket("rawdata").Get(line.Id, line)
}
func (b *BowStorage) GetLineG3(line *model.RawSourceLine) error {
	return b.db.Bucket("rawdata").Get(line.Id, line)
}

func (b *BowStorage) ListBuckets() ([]string, error) {
	buckets := b.db.Buckets()
	sort.Strings(buckets)
	return buckets, nil
}

func (b *BowStorage) GetBucket(name string) (*Iter, error) {
	return b.db.Bucket(name).Iter(), nil
}

func (b *BowStorage) FindBuckets(start, end int64) ([]string, error) {

	monthStart := func(ts int64) time.Time {
		t := time.UnixMilli(ts) //.In(time.UTC)
		return time.Date(
			t.Year(), t.Month(), 1,
			0, 0, 0, 0,
			t.Location(),
		)
	}

	dbBuckets, err := b.ListBuckets()
	if err != nil {
		return nil, err
	}

	ts := monthStart(start)
	tsEnd := monthStart(end).AddDate(0, 1, 0).Add(-time.Second)
	buckets := []string{}
	allBuckets := map[string]bool{}

	for _, b := range dbBuckets {
		allBuckets[b] = true
	}

	for ts.Before(tsEnd) {
		bucketName := ts.Format("200601")
		if _, ok := allBuckets[bucketName]; ok {
			buckets = append(buckets, bucketName)
		}
		ts = ts.AddDate(0, 1, 0)
	}
	sort.Strings(buckets)
	return buckets, nil
}

func GenerateCPKey(year int, month int) string {
	return fmt.Sprintf("CP/%.4d/%.2d", year, month)
}
