package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
	endPoints      = []string{"localhost:2379", "localhost:22379", "localhost:32379"}
)

func main() {
	// Start
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   endPoints,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	kv := clientv3.NewKV(cli)

	// CreateNew(ctx, kv, "crypto", "BUY_VALUE", "100")

	prefix := "general/crypto/version/SELL_VALUE"
	gr, err := GetWithPrefix(ctx, kv, prefix)
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(gr.Kvs) == 0 {
		fmt.Println("No Data Found!")
		return
	}
	for i := 0; i < len(gr.Kvs); i++ {
		fmt.Println("Key: " + string(gr.Kvs[i].Key) + ", Value: " + string(gr.Kvs[i].Value))
	}
}

/* ========== Usecase ========== */
func CreateNew(ctx context.Context, kv clientv3.KV, serviceName string, keyName string, value string) {
	// Get Used:
	usedKey := fmt.Sprintf(serviceName + "/used/" + keyName)
	gr, err := GetOne(ctx, kv, usedKey)
	if err != nil {
		fmt.Println(err)
		return
	}

	// If used key unexist => create used key & v1
	if len(gr.Kvs) == 0 {
		fmt.Println("No Used Key Found")

		// Create Key v1
		newKey := fmt.Sprintf(serviceName + "/version/" + keyName + "/v1")
		_, err := PutOne(ctx, kv, newKey, value)
		if err != nil {
			fmt.Println(err)
			return
		}

		// Create Used Version
		_, err = PutOne(ctx, kv, usedKey, newKey)
		if err != nil {
			fmt.Println(err)
			return
		}

		return
	}
	// If used key exist => create latest version
	keyPrefix := fmt.Sprintf(serviceName + "/version/" + keyName)
	gr, err = GetWithPrefix(ctx, kv, keyPrefix)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key) + "\n" + string(item.Value))
	}

	lastNum := len(gr.Kvs) - 1
	fmt.Println(string(gr.Kvs[lastNum].Key) + "\n" + string(gr.Kvs[lastNum].Value))
}

/* ========== Repo ========== */
func GetOne(ctx context.Context, kv clientv3.KV, key string) (*clientv3.GetResponse, error) {
	gr, err := kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return gr, nil
}

func GetWithPrefix(ctx context.Context, kv clientv3.KV, key string) (*clientv3.GetResponse, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	gr, err := kv.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	return gr, nil
}

func PutOne(ctx context.Context, kv clientv3.KV, key string, value string) (*clientv3.PutResponse, error) {
	pr, err := kv.Put(ctx, key, value)
	if err != nil {
		return nil, err
	}

	return pr, nil
}

// /*
func PutMultipleExample(ctx context.Context, kv clientv3.KV) {
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			k := fmt.Sprintf("key_%02d", i)
			kv.Put(ctx, k, strconv.Itoa(i))
		} else if i%3 == 0 {
			k := fmt.Sprintf("key_%02d", i)
			kv.Put(ctx, k, "true")
		} else {
			k := fmt.Sprintf("key_%02d", i)
			kv.Put(ctx, k, "something")
		}
		// k := fmt.Sprintf("key_%02d", i)
		// kv.Put(ctx, k, strconv.Itoa(i))
	}
}

func Convert() {
	// Start
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   endPoints,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	kv := clientv3.NewKV(cli)

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}

	gr, err := kv.Get(ctx, "key", opts...)
	if err != nil {
		log.Fatal(err)
	}

	var str string
	var integer int
	var boolean bool

	for i := 0; i < len(gr.Kvs); i++ {
		if string(gr.Kvs[i].Value) == "true" {
			boolean = true
		} else if string(gr.Kvs[i].Value) == "false" {
			boolean = false
		} else if num, err := strconv.Atoi(string(gr.Kvs[i].Value)); err == nil {
			integer = num
		} else {
			str = string(gr.Kvs[i].Value)
		}
	}

	fmt.Println(str, integer, boolean)
}

// */
