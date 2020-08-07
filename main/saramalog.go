package main

import logger "github.com/tal-tech/loggerX"

type saramaLog struct {
}

func (this *saramaLog) Print(v ...interface{}) {
	logger.D("SaramaLog", v)
}
func (this *saramaLog) Printf(format string, v ...interface{}) {
	logger.D("SaramLog", format, v...)
}
func (this *saramaLog) Println(v ...interface{}) {
	logger.D("SaramLog", v)
}
