package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"

    "gopkg.in/yaml.v3"
)

const configPath = "./config.yml"

var logger *log.Logger
var config *Config
var wg sync.WaitGroup

func main() {
    //初始化log
    err := initLog()
    if err != nil {
        fmt.Println(err.Error())
        return
    }

    wg.Add(3)
    // 读取配置
    LoadConfig(configPath)
    // 初始化数据库链接

    logger.Println("开始获取数据...")

    // 周期性调用获取并保存数据
    ticker := time.Tick(time.Duration(config.Interval) * time.Millisecond)
    for _ = range ticker {
        bytes, err := fetchData()
        if err != nil {
            logger.Println("获取数据错误：" + err.Error())
            continue
        }

        list := []Data{}
        if err := json.Unmarshal(bytes, &list); err != nil {
            logger.Println("数据转换json错误：" + err.Error())
            continue
        }

        for _, data := range list {
            result, err := storeData(data)
            if err != nil {
                logger.Println("存储数据错误：" + err.Error())
                continue
            } else {
                logger.Println("存储数据成功：" + result.Message)
            }
        }

    }

    wg.Wait()
}

func fetchData() (data []byte, err error) {
    client := &http.Client{
        Timeout: time.Duration(10) * time.Second,
    }

    url := config.DataUrl
    method := "GET"
    req, err := http.NewRequest(method, url, nil)
    if err != nil {
        logger.Println(err)
        return
    }

    res, err := client.Do(req)
    if err != nil {
        logger.Println(err)
        return
    }
    defer res.Body.Close()

    data, err = ioutil.ReadAll(res.Body)
    if err != nil {
        logger.Println(err)
        return
    }

    fmt.Println(string(data))
    return
}

func storeData(data Data) (Result, error) {
    client := &http.Client{
        Timeout: time.Duration(10) * time.Second,
    }

    url := config.PushUrl
    method := "POST"

    v, _ := strconv.ParseFloat(data.Value, 64)
    gas := Gas{
        Ts:    time.Now().Format("2006-01-02 15:04"),
        Value: v,
        Point: data.Name,
        //Point:  data.Name[len(data.Name)-4 : len(data.Name)-1],
        PName:  data.Name,
        Unit:   config.Unitmap[data.Name],
        Region: config.Region,
    }
    b, _ := json.Marshal(gas)

    req, err := http.NewRequest(method, url, bytes.NewReader(b))
    if err != nil {
        return Result{}, err
    }

    req.Header.Add("Content-Type", "application/json")
    res, err := client.Do(req)
    if err != nil {
        return Result{}, err
    }
    defer res.Body.Close()

    bytes, err := ioutil.ReadAll(res.Body)
    if err != nil {
        return Result{}, err
    }

    r := Result{}
    if err := json.Unmarshal(bytes, &r); err != nil {
        logger.Println("result转换json错误：" + err.Error())
        return Result{}, err
    }

    fmt.Println(r)
    return r, nil
}

func initLog() error {
    logger = log.New(os.Stdout, "", log.Lshortfile|log.Ldate|log.Ltime)
    return nil
}

func LoadConfig(configFilePath string) {
    // load config
    data, err := ioutil.ReadFile(configFilePath)
    if err != nil {
        panic(err.Error())
    }

    config = NewConfigWithDefault()
    err = yaml.Unmarshal(data, &config)
    if err != nil {
        logger.Fatalf("error: %v", err)
    }
    fmt.Printf("config db:\n%v\n\n", config)
}

// config 缺省设置
func NewConfigWithDefault() *Config {
    c := &Config{
        DataUrl:  "https://agsi.gie.eu/api?date=",
        PushUrl:  "http://10.22.135.21:8080",
        Region:   "liaohe",
        Interval: 60000,
    }
    return c
}

type Config struct {
    DataUrl  string            `yaml:"dataUrl"`  // api地址
    PushUrl  string            `yaml:"pushUrl"`  // api地址
    Region   string            `yaml:"region"`   // api地址
    Interval int               `yaml:"interval"` // api地址
    Unitmap  map[string]string `yaml:"unitmap"`  // api地址
}

type Data struct {
    Name  string `yaml:"name"` // 执行环境 dev or prod
    Value string `yaml:"value"`
}

type Result struct {
    Code    int    `json:"code"`
    Message string `json:"message"`
    Num     int64  `json:"num"`
}

type Gas struct {
    Ts     string  `json:"ts"`
    Value  float64 `json:"value"`
    Point  string  `json:"point"`
    PName  string  `json:"pname"`
    Unit   string  `json:"unit"`
    Region string  `json:"region"`
}
