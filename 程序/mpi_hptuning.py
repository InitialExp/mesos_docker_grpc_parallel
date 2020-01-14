from mpi4py import rc
rc.initialize = False
from mpi4py import MPI
import sys,threading
import parameter_tuning_pb2
import parameter_tuning_pb2_grpc
import grpc
from concurrent import futures
from trading_system.generate_parameter import generate,print_tree,generate_all_param
from trading_system.engine import Run_func
from sample_hpr import initialize, handle_data
import logging
class hptuning(parameter_tuning_pb2_grpc.hptuningServicer):
    def __init__(self, mpi_size, stop_event):
        self.mpi_size = mpi_size
        self.comm = MPI.COMM_WORLD
        self.result = {}
        self._stop_event=stop_event
        pass

    def gridsearch(self, request, context):


        tasks = {}
        for task_define in request.params:
            print(task_define)
            tasks[task_define.name] = \
                {'start':int(task_define.start),
                 'dt':int(task_define.dt),
                 'step':int(task_define.step)}


        config = {}
        config['benchmark'] = request.task_config.benchmark
        config['expected_return_days'] = request.task_config.expected_return_days
        config['stocks'] = list(request.task_config.stocks)
        config['initial_capital'] = request.task_config.initial_capital
        config['start'] = request.task_config.start
        config['stragety']=request.task_config.stragety
        config['balanced_dates'] = {'method':request.task_config.balancedDays.method,
                                    'param':request.task_config.balancedDays.param}
        config['end'] = request.task_config.end
        config['cleaned_weights']=request.task_config.cleaned_weights


        params, all_param = generate_all_param({'opt_param':tasks})
        num_tasks = len(all_param)
        num_params = len(params)
        numsent=0
        status = MPI.Status()
        for i in range(1, min(self.mpi_size, num_tasks)):
            conf = config.copy()
            for j in range(num_params):
                conf[params[j]] = all_param[i-1][j]
            print(conf, i, '\n\n')
            self.comm.send(conf, dest=i, tag=i)
            numsent+=1
            self.result[i-1]= {'params':all_param[i-1]}


        for i in range(0, num_tasks):
            sharp_recv = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            sender = status.source
            tag = status.tag
            print("tag rec::::::",tag)
            self.result[tag]['result'] = sharp_recv


            if(numsent<num_tasks):
                conf = config.copy()
                for j in range(num_params):
                    conf[params[j]] = all_param[numsent][j]
                self.comm.send(conf, dest=sender, tag=numsent+1)
                self.result[numsent] = {'params': all_param[numsent]}
                numsent+=1

            else:
                self.comm.send(None, dest=sender, tag=0)

        results = parameter_tuning_pb2.hptuning_tasks()
        for key, value in self.result.items():
            task = parameter_tuning_pb2.parameter_result()
            for j in range(num_params):
                para = parameter_tuning_pb2.parameter()
                para.name = params[j]
                para.value=str(value['params'][j])
                task.parameter_setting.append(para)
            metric = parameter_tuning_pb2.metric()
            metric.name = 'sharp'
            metric.value = value['result']
            task.parameter_metrics.append(metric)
            results.tasks.append(task)

        print("hahaK:",results)
        return results


    def stopServer(self, request, context):

        self.comm.barrier()
        MPI.Finalize()
        self._stop_event.set()
        return request


def serve(mpi_size):
    stop_event = threading.Event()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    parameter_tuning_pb2_grpc.add_hptuningServicer_to_server(hptuning(mpi_size, stop_event), server)
    server.add_insecure_port('[::]:{port}'.format(port=50051))
    print("server stated at port:", 50051)
    server.start()
    stop_event.wait()
    server.stop(None)


if __name__ == "__main__":


    MPI.Init()
    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        logging.basicConfig()
        serve(size)
        '''
        para_define = parameter_tuning_pb2.para_define()
        para_define.name = 'expected_return_days'
        para_define.start = 20
        para_define.dt = 3
        para_define.step = 4

        opt = parameter_tuning_pb2.config()
        opt.benchmark = '000001.SH'
        opt.expected_return_days = 40
        opt.stocks.extend(
            ['000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ', '000007.SZ', '000008.SZ', '000009.SZ',
             '000010.SZ'])
        opt.initial_capital = 1000000
        opt.start = 20180101
        opt.end = 20181230
        opt.stragety = 'sample_hpr'

        balanced_dates = parameter_tuning_pb2.balanced_dates()
        balanced_dates.param = 20
        balanced_dates.method = 'equal_difference'
        opt.balancedDays.MergeFrom(balanced_dates)

        opt.cleaned_weights = True

        tasks = parameter_tuning_pb2.tasks_difine()
        tasks.params.append(para_define)

        tasks.task_config.MergeFrom(opt)
        c = hptuning(size)
        c.gridsearch(tasks, 0)
        '''


    else:
        status = MPI.Status()

        config = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        print("worker receive task:", rank, config,tag)
        while(tag>0):
            print("begin calc,", tag)
            sharp = Run_func(initialize, handle_data, config)['sys_analyser']['summary']['sharpe']
            comm.send(sharp, dest=0, tag=tag-1)
            config = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag =status.Get_tag()
            print("worker receive task:", rank, config,tag)
        comm.barrier()

        MPI.Finalize()












