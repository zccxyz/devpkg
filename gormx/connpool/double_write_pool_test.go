package connpool

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"test/webook/pkg/logger"
	"testing"
	"time"
)

type DoubleWritePoolTestSuite struct {
	suite.Suite
	db *gorm.DB
}

func (d *DoubleWritePoolTestSuite) SetupSuite() {
	src, err := gorm.Open(mysql.Open(`root:root@tcp(127.0.0.1:3306)/webook?charset=utf8mb4`))
	require.NoError(d.T(), err)
	err = src.AutoMigrate(&TestTable{})
	require.NoError(d.T(), err)

	dst, err := gorm.Open(mysql.Open(`root:root@tcp(127.0.0.1:3306)/webook_interactive?charset=utf8mb4`))
	require.NoError(d.T(), err)
	err = dst.AutoMigrate(&TestTable{})
	require.NoError(d.T(), err)

	pool := NewDoubleWritePool(src, dst, logger.NewNoLogger())
	err = pool.UpdatePattern(PatternSrcFirst)
	require.NoError(d.T(), err)
	db, err := gorm.Open(mysql.New(

		mysql.Config{
			Conn: pool,
		},
	))
	require.NoError(d.T(), err)
	d.db = db
}

func (d *DoubleWritePoolTestSuite) TearDownTest() {
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//d.db.WithContext(ctx).Exec("DROP TABLE test_tables")
}

func (d *DoubleWritePoolTestSuite) TestDoubleWrite() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now().UnixMilli()
	err := d.db.WithContext(ctx).Create(&TestTable{
		Name:   "xyz",
		Age:    100,
		Gender: 1,
		CTime:  now,
		UTime:  now,
	}).Error
	require.NoError(d.T(), err)
}

func (d *DoubleWritePoolTestSuite) TestDoubleWriteTx() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now().UnixMilli()
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(&TestTable{
			Name:   "xyz2",
			Age:    200,
			Gender: 2,
			CTime:  now,
			UTime:  now,
		}).Error
	})
	require.NoError(d.T(), err)
}

func TestDoubleWritePool(t *testing.T) {
	suite.Run(t, new(DoubleWritePoolTestSuite))
}

type TestTable struct {
	Id     int64 `gorm:"primaryKey,autoIncrement"`
	Name   string
	Age    int
	Gender int
	CTime  int64
	UTime  int64
}
