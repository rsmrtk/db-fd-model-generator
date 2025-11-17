package logger

import (
	"log"
	"os"

	"github.com/fatih/color"
)

// H is a shorthand for map[string]interface{}
type H map[string]interface{}

type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Fatalln(v ...interface{})
	Errorln(v ...interface{})
	Warningln(v ...interface{})
	Error(msg string, fields H)
}

type logger struct {
	// Text colors
	magenta func(a ...interface{}) string
	// Background colors
	byellow  func(a ...interface{}) string
	bred     func(a ...interface{}) string
	bcyan    func(a ...interface{}) string
	bmagenta func(a ...interface{}) string

	hiWhite func(a ...interface{}) string

	lgr *log.Logger
}

func (l *logger) Printf(format string, v ...interface{}) {
	l.lgr.Printf(l.bcyan("INFO")+" "+l.hiWhite(format), v...)
}

func (l *logger) Println(v ...interface{}) {
	l.lgr.Println(l.bcyan("INFO"), l.hiWhite(v...))
}

func (l *logger) Fatalln(v ...interface{}) {
	l.lgr.Println(l.bred("ERR"), l.hiWhite(v...))
	l.lgr.Println(l.bred("OS_EXIT"), l.hiWhite("Exiting..."))
}

func (l *logger) Errorln(v ...interface{}) {
	l.lgr.Println(l.bred("ERR"), l.hiWhite(v...))
}

func (l *logger) Warningln(v ...interface{}) {
	l.lgr.Println(l.byellow("WARN"), l.hiWhite(v...))
}

func (l *logger) Error(msg string, fields H) {
	l.lgr.Printf("%s %s %v", l.bred("ERR"), l.hiWhite(msg), fields)
}

func New() Logger {
	magenta := color.New(color.FgMagenta).SprintFunc()

	l := log.New(os.Stderr, magenta("[Model Generator] "), log.Ltime)

	backgroundMagenta := color.New(color.BgMagenta).SprintFunc()
	backgroundCyan := color.New(color.BgCyan).SprintFunc()
	backgroundRed := color.New(color.BgRed).SprintFunc()
	backgroundYellow := color.New(color.BgYellow).SprintFunc()

	hiWhite := color.New(color.FgHiWhite).SprintFunc()

	return &logger{
		byellow:  backgroundYellow,
		bred:     backgroundRed,
		bcyan:    backgroundCyan,
		bmagenta: backgroundMagenta,

		magenta: magenta,

		hiWhite: hiWhite,
		lgr:     l,
	}
}
