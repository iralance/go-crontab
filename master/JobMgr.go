package master

import (
	"context"
	"encoding/json"
	"github.com/iralance/go-crontab/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	//初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	//建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	//把任务保存在/cron/jobs/任务名 -> json
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + job.Name

	// 任务信息json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果是更新, 那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return

}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	//把任务保存在/cron/jobs/任务名 -> json
	var (
		jobKey     string
		deleteResp *clientv3.DeleteResponse
		oldJobObj  common.Job
	)
	jobKey = common.JOB_SAVE_DIR + name

	// 保存到etcd
	if deleteResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果是更新, 那么返回旧值
	if len(deleteResp.PrevKvs) > 0 {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return

}

func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	//把任务保存在/cron/jobs/任务名 -> json
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		job     *common.Job
	)
	dirKey = common.JOB_SAVE_DIR

	// 保存到etcd
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	jobList = make([]*common.Job, 0)
	// 如果是更新, 那么返回旧值
	if len(getResp.Kvs) > 0 {
		// 对旧值做一个反序列化
		for _, kv = range getResp.Kvs {
			if job, err = common.UnpackJob(kv.Value); err != nil {
				continue
			}
			jobList = append(jobList, job)
		}

	}
	return

}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名

	var (
		killerKey      string
		leaseId        clientv3.LeaseID
		leaseGrantResp *clientv3.LeaseGrantResponse
	)
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return

}
