import unittest

from styx.common.run_func_payload import RunFuncPayload

from worker.sequencer.sequencer import Sequencer
from worker.sequencer.calvin_sequencer import CalvinSequencer


class TestSequencer(unittest.TestCase):

    def test_sequencer(self):
        test_payload = RunFuncPayload(b'0', 0, 0, 'test', 0, 'test', (0, ), None, None)
        seq = Sequencer()
        seq.set_worker_id(1)
        seq.set_n_workers(2)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        epoch = seq.get_epoch()
        assert [sq_item.t_id for sq_item in epoch] == [1, 3, 5, 7]

        concurrency_aborts = {1, 3}
        logic_aborts = {3}

        abr = [sq_item.t_id for sq_item in seq.get_aborted_sequence(concurrency_aborts_everywhere=concurrency_aborts,
                                                                    logic_aborts_everywhere=logic_aborts)]

        assert abr == [1]

        seq.increment_epoch(remote_t_counters=[seq.t_counter+1],
                            concurrency_aborts_everywhere=concurrency_aborts,
                            logic_aborts_everywhere=logic_aborts)
        seq.sequence(test_payload)
        epoch = seq.get_epoch()

        assert [sq_item.t_id for sq_item in epoch] == [1, 11]

    def test_vanilla_sequencer(self):
        test_payload1 = RunFuncPayload(b'0', 0, 0, 'test', 0, 'test', (0,), None, None)
        test_payload2 = RunFuncPayload(b'1', 1, 1, 'test', 1, 'test', (1,), None, None)
        test_payload3 = RunFuncPayload(b'2', 2, 2, 'test', 2, 'test', (2,), None, None)
        n1 = 3
        n2 = 3
        n3 = 3
        seq1 = CalvinSequencer()
        seq2 = CalvinSequencer()
        seq3 = CalvinSequencer()
        for _ in range(n1):
            seq1.sequence(test_payload1)
        for _ in range(n2):
            seq2.sequence(test_payload2)
        for _ in range(n3):
            seq3.sequence(test_payload3)
        s1 = seq1.get_epoch()
        s2 = seq2.get_epoch()
        s3 = seq3.get_epoch()
        payloads = {1: s1, 2: s2, 3: s3}
        lens = {key: len(pld) for key, pld in payloads.items()}
        counters = {key: 0 for key in payloads.keys()}
        total_len = sum(lens.values())
        def generate_round_robin_idx():
            i = 0
            while i < total_len:
                for worker in lens.keys():
                    if counters[worker] < lens[worker]:
                        yield worker, counters[worker], i
                        i += 1
                        counters[worker] += 1

        rr_gen = generate_round_robin_idx()
        for _ in range(total_len):
            print(next(rr_gen))

    def test_four_sequencers(self):
        test_payload = RunFuncPayload(b'0', 0, 0, 'test', 0, 'test', (0,), None, None)
        seq1 = Sequencer(max_size=2)
        seq2 = Sequencer(max_size=2)
        seq3 = Sequencer(max_size=2)
        seq4 = Sequencer(max_size=2)
        seq1.set_worker_id(1)
        seq2.set_worker_id(2)
        seq3.set_worker_id(3)
        seq4.set_worker_id(4)
        seq1.set_n_workers(4)
        seq2.set_n_workers(4)
        seq3.set_n_workers(4)
        seq4.set_n_workers(4)
        for i in range(4):
            seq1.sequence(test_payload)
        for i in range(3):
            seq2.sequence(test_payload)
        for i in range(5):
            seq3.sequence(test_payload)
        for i in range(2):
            seq4.sequence(test_payload)
        e1 = seq1.get_epoch()
        print([sq_item.t_id for sq_item in e1])
        e2 = seq2.get_epoch()
        print([sq_item.t_id for sq_item in e2])
        e3 = seq3.get_epoch()
        print([sq_item.t_id for sq_item in e3])
        e4 = seq4.get_epoch()
        print([sq_item.t_id for sq_item in e4])
        seq1.increment_epoch([3, 5, 2])
        seq2.increment_epoch([4, 5, 2])
        seq3.increment_epoch([4, 3, 2])
        seq4.increment_epoch([4, 3, 5])
        for i in range(4):
            seq1.sequence(test_payload)
        for i in range(3):
            seq2.sequence(test_payload)
        for i in range(5):
            seq3.sequence(test_payload)
        for i in range(2):
            seq4.sequence(test_payload)
        e1 = seq1.get_epoch()
        print([sq_item.t_id for sq_item in e1])
        e2 = seq2.get_epoch()
        print([sq_item.t_id for sq_item in e2])
        e3 = seq3.get_epoch()
        print([sq_item.t_id for sq_item in e3])
        e4 = seq4.get_epoch()
        print([sq_item.t_id for sq_item in e4])
        e1 = seq1.get_epoch()
        print([sq_item.t_id for sq_item in e1])
        e2 = seq2.get_epoch()
        print([sq_item.t_id for sq_item in e2])
        e3 = seq3.get_epoch()
        print([sq_item.t_id for sq_item in e3])
        e4 = seq4.get_epoch()
        print([sq_item.t_id for sq_item in e4])
        e1 = seq1.get_epoch()
        print([sq_item.t_id for sq_item in e1])
        e2 = seq2.get_epoch()
        print([sq_item.t_id for sq_item in e2])
        e3 = seq3.get_epoch()
        print([sq_item.t_id for sq_item in e3])
        e4 = seq4.get_epoch()
        print([sq_item.t_id for sq_item in e4])
