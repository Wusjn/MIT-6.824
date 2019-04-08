package main
import "time"
import "fmt"

func main(){
    fmt.Printf("%s\n",time.Now())
    timer := time.NewTimer(time.Second * 3)
    go func(){
        time.Sleep(time.Second)
        timer.Stop()
        timer.Reset(time.Second * 3)
    }()
    for{
        select{
        case <- timer.C:
           fmt.Printf("fire at %s\n", time.Now())
        default:
        }
    }
}