package geecache

import (
	"fmt"
	"geecache/consistenthash"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	//用来记录自己的地址，包括主机名/IP 和端口
	self string
	//作为节点间通讯地址的前缀，默认是 /_geecache/
	basePath string
	mu sync.Mutex
	//类型是一致性哈希算法的 Map，用来根据具体的 key 选择节点
	peers *consistenthash.Map
	/*映射远程节点与对应的 httpGetter。
	 *每一个远程节点对应一个 httpGetter，
	 *因为 httpGetter 与远程节点的地址 baseURL 有关
	 *e.g. "keyed by http://10.0.0.2:8008"
	 */
	httpGetters map[string]*httpGetter
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self : self,
		basePath: defaultBasePath,
	}
}

func (p *HTTPPool) Log(format string,v ...interface{}) {
	log.Printf("[Server %s] %s",p.self,fmt.Sprintf(format,v...))
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter,r *http.Request) {
	if !strings.HasPrefix(r.URL.Path,p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

	p.Log("%s %s",r.Method,r.URL.Path)
	parts := strings.SplitN(r.URL.Path[len(p.basePath):],"/",2)
	if len(parts) != 2{
		http.Error(w,"bad request",http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}

//实例化了一致性哈希算法，并且添加了传入的节点
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter,len(peers))
	for _,peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer+p.basePath}
	}
}

//包装了一致性哈希算法的 Get() 方法，根据具体的 key，选择节点，
//返回节点对应的 HTTP 客户端
func (p *HTTPPool) PickPeer(key string) (PeerGetter,bool){
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer !=""&&peer != p.self {
		p.Log("Pick Peer %s",peer)
		return p.httpGetters[peer],true
	}
	return nil,false
}

var _ PeerGetter = (*httpGetter)(nil)

type httpGetter struct {
	//将要访问的远程节点的地址，例如 http://example.com/_geecache/
	baseURL string
}

//使用 http.Get() 方式获取返回值，并转换为 []bytes 类型
func (h *httpGetter) Get(group string,key string) ([]byte,error) {
	u := fmt.Sprintf(
		/*
		 *查询字符串是URL的一部分，其中包含可传递给Web应用程序的数据。
		 *这些数据需要进行编码，并使用 url.QueryEscape 完成此编码。
		 *它执行通常也称为网址编码的内容
		 *假设我们有网页： http://mywebpage.com/thumbify 
		 *我们想要将图片url http://images.com/cat.png 传递给此Web应用程序。然后，
		 *这个网址需要如下所示： http://mywebpage.com/thumbify?image=http%3A%2F ％2Fimages.com％2Fcat.png 
		 *在Go代码中，它看起来像这样
		 *
		 *网页：="http://mywebpage.com/thumbify"
		 *图片：="http://images.com /cat.png"
 		 *fmt.Println（网页+"？image ="+ url.QueryEscape（图片））
		 */
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(group),
		url.QueryEscape(key),
	)
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil
}

var _ PeerGetter = (*httpGetter)(nil)