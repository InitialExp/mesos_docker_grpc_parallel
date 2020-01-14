import grpc, parameter_tuning_pb2_grpc, parameter_tuning_pb2
import time
if __name__ == "__main__":
    with grpc.insecure_channel("node1:31000") as channel:
        para_define = parameter_tuning_pb2.para_define()
        para_define.name = 'expected_return_days'
        para_define.start = 20
        para_define.dt = 12
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

        dur0 = time.time()

        stub = parameter_tuning_pb2_grpc.hptuningStub(channel)
        results = stub.gridsearch(tasks)
        dur1 = time.time()
        flag = parameter_tuning_pb2.flag()
        stop = stub.stopServer(flag)
        print(results)

        print("calculation duration（3）:", dur1-dur0)
