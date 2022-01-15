import signal
import asyncio
import traceback
import socket
import os
import sys
import multiprocessing
import faulthandler

import uvloop

signames = {
    int(v): v.name for k, v in signal.__dict__.items()
    if isinstance(v, signal.Signals)}


class Application:
    def __init__(self, *, reaper_settings=None, log_request=None,
                 protocol_factory=None, debug=False):
        self._loop = None
        self._connections = set()
        self._reaper_settings = reaper_settings or {}
        self._error_handlers = []
        self._log_request = log_request
        self._request_extensions = {}
        self._protocol_factory = protocol_factory
        self._debug = debug

    @property
    def loop(self):
        if not self._loop:
            self._loop = uvloop.new_event_loop()

        return self._loop

    def serve(self, *, sock, host, port, reloader_pid):
        faulthandler.enable()
        self.__finalize()

        loop = self.loop
        asyncio.set_event_loop(loop)

        server_coro = loop.create_server(
            lambda: self._protocol_factory(self), sock=sock)

        server = loop.run_until_complete(server_coro)

        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.add_signal_handler(signal.SIGINT, loop.stop)

        print('Accepting connections on http://{}:{}'.format(host, port))

        try:
            loop.run_forever()
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.run_until_complete(self.drain())
            self._reaper.stop()
            loop.close()

            # break reference and cleanup matcher buffer
            del self._matcher

    def _run(self, *, host, port, worker_num=None, reloader_pid=None,
             debug=None):
        self._debug = debug or self._debug
        if self._debug and not self._log_request:
            self._log_request = self._debug

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        os.set_inheritable(sock.fileno(), True)

        workers = set()

        terminating = False

        def stop(sig, frame):
            nonlocal terminating
            if reloader_pid and sig == signal.SIGHUP:
                print('Reload request received')
            elif not terminating:
                terminating = True
                print('Termination request received')
            for worker in workers:
                worker.terminate()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGHUP, stop)

        for _ in range(worker_num or 1):
            worker = multiprocessing.Process(
                target=self.serve,
                kwargs=dict(sock=sock, host=host, port=port,
                            reloader_pid=reloader_pid))
            worker.daemon = True
            worker.start()
            workers.add(worker)

        # prevent further operations on socket in parent
        sock.close()

        for worker in workers:
            worker.join()

            if worker.exitcode > 0:
                print('Worker exited with code {}'.format(worker.exitcode))
            elif worker.exitcode < 0:
                try:
                    signame = signames[-worker.exitcode]
                except KeyError:
                    print(
                        'Worker crashed with unknown code {}!'
                            .format(worker.exitcode))
                else:
                    print('Worker crashed on signal {}!'.format(signame))

    def run(self, host='0.0.0.0', port=20880, *, worker_num=None):

        self._run(
            host=host, port=port, worker_num=worker_num)
