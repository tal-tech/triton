package stream

import (
	"context"
	"database/sql"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cast"
	logger "github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
)

type SqlStream struct {
	dbs map[string]*sql.DB
}

func NewSqlStream() Stream {
	this := new(SqlStream)
	this.dbs = make(map[string]*sql.DB)
	confs := confutil.GetConfStringMap("MysqlCluster")
	for key, conf := range confs {
		if db, err := sql.Open("mysql", conf); err != nil {
			logger.E("MYSQL_ERROR", "CreateMysqlError:%v", err)
		} else {
			this.dbs[key] = db
		}
	}
	return this
}

func (this *SqlStream) Operate(ctx context.Context, cmds []string) (ret bool) {
	ss := this.rebuildSql(cmds)
	db, ok := this.dbs[ss.instance]
	if !ok {
		logger.E("SQL", "instance:%s not found", ss.instance)
		return false
	}
	if tx, err := db.Begin(); err == nil {
		success := 0
		for _, sql := range ss.sqls {
			if strings.Contains(sql, "select") {
				rs, e := tx.Query(sql)
				var qs []string
				if e == nil {
					for rs.Next() {
						cs, _ := rs.Columns()
						a := make([]string, len(cs))
						var b []interface{}
						for i, _ := range a {
							b = append(b, &a[i])
						}
						rs.Scan(b...)
						cols := cast.ToStringSlice(b)
						qs = append(qs, strings.Join(cols, ","))
					}
				}
				qret := strings.Join(qs, "|")
				logger.I("SQL", "sql=[%s] ret=[%s]", sql, qret)
			}
			if ret, err := tx.Exec(sql); err == nil {
				success++
				affected, _ := ret.RowsAffected()
				logger.I("SQL", "sql:%s,affect:%v", sql, affected)
			} else {
				logger.E("SQL", "sql:%s,err:%v", sql, err)
				break
			}
		}
		if success == len(ss.sqls) {
			if err := tx.Commit(); err == nil {
				return true
			} else {
				logger.E("SQL_COMMIT", "err:%v,cmds:%v", err, cmds)
			}
		}
		if err := tx.Rollback(); err != nil {
			logger.E("SQL_COMMIT", "rollback,err:%v", err)
		}
	} else {
		logger.E("TransactionError", "err:%v", err)
	}
	return false
}

type SqlSession struct {
	instance string
	sqls     []string
}

func (this *SqlStream) rebuildSql(cmds []string) SqlSession {
	sql := ""
	tt := len(cmds)
	sqls := make([]string, 0)
	instance := "default"
	for i := 0; i < tt; i++ {
		if cmds[i] == "@SQL" {
			next := cmds[i+1]
			if strings.HasPrefix(next, "USE:") {
				instance = next[4:]
				i = i + 1
			}
		} else {
			sql = sql + " " + cmds[i]
			if strings.HasSuffix(cmds[i], ";") {
				sqls = append(sqls, sql)
				sql = ""
			}
		}
	}
	var ss SqlSession
	ss.sqls = sqls
	ss.instance = instance + ".writer"
	return ss
}
