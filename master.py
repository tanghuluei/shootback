#!/usr/bin/env python3
# coding=utf-8
import os
import tempfile
import queue
import atexit
from common_func import *

_listening_sockets = []  # for close at exit
__author__ = "Aploium <i@z.codes>"
__website__ = "https://github.com/aploium/shootback"

_DEFAULT_SSL_KEY = '''\
-----BEGIN PRIVATE KEY-----
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
DELPZEjMnppixdHVcyzpfhieGu7D+Q==
-----END PRIVATE KEY-----
'''
_DEFAULT_SSL_CERT = '''\
-----BEGIN CERTIFICATE-----
MIIFHzCCAwegAwIBAgIUdvRiUd1V6EvaGQ3MFksairDGrf8wDQYJKoZIhvcNAQEN
BQAwLDEOMAwGA1UEAwwFNjY2NjYxDTALBgNVBAoMBHJvb3QxCzAJBgNVBAYTAkNO
MCAXDTI0MDgxNzE2NDMwMFoYDzIwOTkxMTI2MTA0MDAwWjBGMQswCQYDVQQGEwJD
TjELMAkGA1UECAwCaGIxCzAJBgNVBAcMAndoMQ4wDAYDVQQKDAV6dXpoaTENMAsG
A1UEAwwEcm9vdDCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBALPQGy/o
91c13djoLEUM5CXCiKiMaVqKR0RHUoiSVjyggq4ovUT2dMKs7XrkD8OJSvArLbla
0b18DsgAJzfuDX+t+6wJPNC4rBM8/pknqojP4klbo0pz6OquqZ57hXKw17w6sGsW
ne1ig1yYty+a+egOgt9+qG2ooRTDKJbG8Mdxr26Xirw6/5BqLi8bqfGyzHQlw93e
M4eJC1j4w7Xn1WMpPklPhGeNe7pwVSkJkUwHQIij5X2Jlhcp7QOnNEXUsV9m0UL0
5WgX1PXc/yA23SILUDt0t3CEkQrqZWtfzIDXtN0HCAq651nwHGWJwASa8SFz4Y9G
sOkDAt8zJ2AqS99Q0NkPEV34DWbYXg4rxxeHmvXMZKgZ8bdPwOtL+a55kTb++sMV
cB4g7Kq2cybGCp4aRwUWIc7XzIHlA2aMhYGTPwrvNl+1uZ9D5p72wdgwQC6+JIq0
ZENJ6fR/BWSjVEeeiohTx3tOIhsC/6ymo1Rdtz7eUutk9UVa8pjw8sHwTD04y3gR
qyh7qiLOweuCiADJhbM5z7H+z57yg8PqKWtp6vzax+7qdA7H3wyjkIs8CxsiXKyu
l8jthssnqNqgtePvp56q/CLh5PbQZy2416T5q5CHyAJqsOd0KDRB7KswHVY/jZv3
7sA5eVFnK8CqHBlBrn+3TSSTQjjYZ5XHVEJzAgMBAAGjHTAbMAsGA1UdEQQEMAKC
ADAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBDQUAA4ICAQCEAbpSyU0gF+G9DFb3
YcoLK5jOI1+C7MBMQPzxDf8eKOiVTesg8PoehXhvMkmieCNPlFGEw2vWALk/Lmh5
5p7rFFBxVrX0e3qZxkPfQ2R/fbRp+x3UKmo4+wl7+P5bleSQ7AXjhLvBLTMMx0EA
3ilTkUWvX8U6KSBAKAtDyzwnDcRykxNLxdHrff32wpyFPFeyKDD6y3eWRj2D2TFQ
CexWCPWnjFPa0E+r8vez6zBI9/Nqe1XSmdfM7QsFRu4Ge2iGLiRWhS4Qa5W/tHY3
sDd1jlayuXARy/pa07Bahlv9umtlPPUlfBuph3IeclyOsl33aG//Lw/htUHOMwqy
AoByz0LT3g0Sv7Go5Ge/9wovJ9iTCznpMz7Y0BTPkMnbkixpiOZ5rVv0CAXbQqYg
9sP4AWivWUtni3gQICtkmnwONkM0cizxTBrt60m48B75YHqXnzUSPyrFUqViZ5BQ
su8vwgPdmztlZn/uE9NRhNbcejajfl24Vcd8z7qkfvd39d3EMqt2Izpi30iQY5m+
tNTUMzgdZ+QqvjoJ4lsLu7XKARC5/2YFaz9KNh0TSTMJrlbtTdiv+i5Z/A2jEr3P
Lg5PmUTOvvDzezaKsW64NmxJ/4JPPOu1N4xzMfwhVGk+Ue0uEyyWAzHRaU+AGcZL
X7jXyBJf/aMdXMV4wHx5UWJKjw==
-----END CERTIFICATE-----
'''


@atexit.register
def close_listening_socket_at_exit():
    log.info("exiting...")
    for s in _listening_sockets:
        log.info("closing: {}".format(s))
        try_close(s)


def try_bind_port(sock, addr):
    while True:
        try:
            sock.bind(addr)
        except Exception as e:
            log.error(("unable to bind {}, {}. If this port was used by the recently-closed shootback itself\n"
                       "then don't worry, it would be available in several seconds\n"
                       "we'll keep trying....").format(addr, e))
            log.debug(traceback.format_exc())
            time.sleep(3)
        else:
            break


class Master(object):
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None, working_pool=None,
                 ssl=False
                 ):
        """

        :param customer_listen_addr: equals to the -c/--customer param
        :param communicate_addr: equals to the -m/--master param
        """
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

        self.working_pool = working_pool or {}

        self.socket_bridge = SocketBridge()

        # a queue for customers who have connected to us,
        #   but not assigned a slaver yet
        self.pending_customers = queue.Queue()

        if ssl:
            self.ssl_context = self._make_ssl_context()
            self.ssl_avail = self.ssl_context is not None
        else:
            self.ssl_avail = False
            self.ssl_context = None

        self.communicate_addr = communicate_addr

        _fmt_communicate_addr = fmt_addr(self.communicate_addr)

        if slaver_pool:
            # 若使用外部slaver_pool, 就不再初始化listen
            # 这是以后待添加的功能
            self.external_slaver = True
            self.thread_pool["listen_slaver"] = None
        else:
            # 自己listen来获取slaver
            self.external_slaver = False
            self.slaver_pool = collections.deque()
            # prepare Thread obj, not activated yet
            self.thread_pool["listen_slaver"] = threading.Thread(
                target=self._listen_slaver,
                name="listen_slaver-{}".format(_fmt_communicate_addr),
                daemon=True,
            )

        # prepare Thread obj, not activated yet
        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare Thread obj, not activated yet
        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare assign_slaver_daemon
        self.thread_pool["assign_slaver_daemon"] = threading.Thread(
            target=self._assign_slaver_daemon,
            name="assign_slaver_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

    def serve_forever(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()
        self.thread_pool["assign_slaver_daemon"].start()
        self.thread_pool["socket_bridge"] = self.socket_bridge.start_as_daemon()

        while True:
            time.sleep(10)

    def _make_ssl_context(self):
        if ssl is None:
            log.warning('ssl module is NOT valid in this machine! Fallback to plain')
            return None

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)  #我更正的,用SSLContext来创建ssl上下文
        #ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        #ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        ctx.verify_mode = ssl.CERT_NONE

        _certfile = tempfile.mktemp()
        with open(_certfile, 'w') as fw:
            fw.write(_DEFAULT_SSL_CERT)
        _keyfile = tempfile.mktemp()
        with open(_keyfile, 'w') as fw:
            fw.write(_DEFAULT_SSL_KEY)
        ctx.load_cert_chain(_certfile, _keyfile)
        os.remove(_certfile)
        os.remove(_keyfile)

        return ctx

    def _transfer_complete(self, addr_customer):
        """a callback for SocketBridge, do some cleanup jobs"""
        log.info("customer complete: {}".format(addr_customer))
        del self.working_pool[addr_customer]

    def _serve_customer(self, conn_customer, conn_slaver):
        """put customer and slaver sockets into SocketBridge, let them exchange data"""
        self.socket_bridge.add_conn_pair(
            conn_customer, conn_slaver,
            functools.partial(  # it's a callback
                # 这个回调用来在传输完成后删除工作池中对应记录
                self._transfer_complete,
                conn_customer.getpeername()
            )
        )

    @staticmethod
    def _send_heartbeat(conn_slaver):
        """send and verify heartbeat pkg"""
        conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        pkg, verify = CtrlPkg.recv(
            conn_slaver, expect_ptype=CtrlPkg.PTYPE_HEART_BEAT)  # type: CtrlPkg,bool

        if not verify:
            return False

        if pkg.prgm_ver < 0x000B:
            # shootback before 2.2.5-r10 use two-way heartbeat
            #   so there is no third pkg to send
            pass
        else:
            # newer version use TCP-like 3-way heartbeat
            #   the older 2-way heartbeat can't only ensure the
            #   master --> slaver pathway is OK, but the reverse
            #   communicate may down. So we need a TCP-like 3-way
            #   heartbeat
            conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        return verify

    def _heart_beat_daemon(self):
        """

        每次取出slaver队列头部的一个, 测试心跳, 并把它放回尾部.
            slaver若超过 SPARE_SLAVER_TTL 秒未收到心跳, 则会自动重连
            所以睡眠间隔(delay)满足   delay * slaver总数  < TTL
            使得一轮循环的时间小于TTL,
            保证每个slaver都在过期前能被心跳保活
        """
        default_delay = 5 + SPARE_SLAVER_TTL // 12
        delay = default_delay
        log.info("heart beat daemon start, delay: {}s".format(delay))
        while True:
            time.sleep(delay)
            # log.debug("heart_beat_daemon: hello! im weak")

            # ---------------------- preparation -----------------------
            slaver_count = len(self.slaver_pool)
            if not slaver_count:
                log.warning("heart_beat_daemon: sorry, no slaver available, keep sleeping")
                # restore default delay if there is no slaver
                delay = default_delay
                continue
            else:
                # notice this `slaver_count*2 + 1`
                # slaver will expire and re-connect if didn't receive
                #   heartbeat pkg after SPARE_SLAVER_TTL seconds.
                # set delay to be short enough to let every slaver receive heartbeat
                #   before expire
                delay = 1 + SPARE_SLAVER_TTL // max(slaver_count * 2 + 1, 12)

            # pop the oldest slaver
            #   heartbeat it and then put it to the end of queue
            slaver = self.slaver_pool.popleft()
            addr_slaver = slaver["addr_slaver"]

            # ------------------ real heartbeat begin --------------------
            start_time = time.perf_counter()
            try:
                hb_result = self._send_heartbeat(slaver["conn_slaver"])
            except Exception as e:
                log.warning("error during heartbeat to {}: {}".format(
                    fmt_addr(addr_slaver), e))
                log.debug(traceback.format_exc())
                hb_result = False
            finally:
                time_used = round((time.perf_counter() - start_time) * 1000.0, 2)
            # ------------------ real heartbeat end ----------------------

            if not hb_result:
                log.warning("heart beat failed: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                try_close(slaver["conn_slaver"])
                del slaver["conn_slaver"]

                # if heartbeat failed, start the next heartbeat immediately
                #   because in most cases, all 5 slaver connection will
                #   fall and re-connect in the same time
                delay = 0

            else:
                log.debug("heartbeat success: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                self.slaver_pool.append(slaver)

    def _handshake(self, conn_slaver):
        """
        handshake before real data transfer
        it ensures:
            1. client is alive and ready for transmission
            2. client is shootback_slaver, not mistakenly connected other program
            3. verify the SECRET_KEY, establish SSL
            4. tell slaver it's time to connect target

        handshake procedure:
            1. master hello --> slaver
            2. slaver verify master's hello
            3. slaver hello --> master
            4. (immediately after 3) slaver connect to target
            4. master verify slaver
            5. [optional] establish SSL
            6. enter real data transfer

        Args:
            conn_slaver (socket.socket)
        Return:
            socket.socket|ssl.SSLSocket: socket obj(may be ssl-socket) if handshake success, else None
        """
        conn_slaver.send(CtrlPkg.pbuild_hs_m2s(ssl_avail=self.ssl_avail).raw)

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return None

        pkg, correct = CtrlPkg.decode_verify(buff, CtrlPkg.PTYPE_HS_S2M)  # type: CtrlPkg,bool

        if not correct:
            return None

        if not self.ssl_avail or pkg.data[1] == CtrlPkg.SSL_FLAG_NONE:
            if self.ssl_avail:
                log.warning('client %s not enabled SSL, fallback to plain.', conn_slaver.getpeername())
            return conn_slaver
        else:
            ssl_conn_slaver = self.ssl_context.wrap_socket(conn_slaver, server_side=True)  # type: ssl.SSLSocket
            log.debug('ssl established slaver: %s', ssl_conn_slaver.getpeername())
            return ssl_conn_slaver

    def _get_an_active_slaver(self):
        """get and activate an slaver for data transfer"""
        try_count = 100
        while True:
            if not try_count:
                return None
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                time.sleep(0.02)
                try_count -= 1
                if try_count % 10 == 0:
                    log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))
                continue

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                # this returned conn may be ssl-socket or plain socket
                actual_conn = self._handshake(conn_slaver)
            except Exception as e:
                log.warning("Handshake failed. %s %s", dict_slaver["addr_slaver"], e)
                log.debug(traceback.format_exc())
                actual_conn = None

                try_count -= 1
                if try_count % 10 == 0:
                    log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))

            if actual_conn is not None:
                return actual_conn
            else:
                log.warning("slaver handshake failed: %s", dict_slaver["addr_slaver"])
                try_close(conn_slaver)

                time.sleep(0.02)

    def _assign_slaver_daemon(self):
        """assign slaver for customer"""
        while True:
            # get a newly connected customer
            conn_customer, addr_customer = self.pending_customers.get()

            try:
                conn_slaver = self._get_an_active_slaver()
            except:
                log.error('error in getting slaver', exc_info=True)
                continue
            if conn_slaver is None:
                log.warning("Closing customer[%s] because no available slaver found", addr_customer)
                try_close(conn_customer)

                continue
            else:
                log.debug("Using slaver: %s for %s", conn_slaver.getpeername(), addr_customer)

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
            }

            try:
                self._serve_customer(conn_customer, conn_slaver)
            except:
                log.error('error adding to socket_bridge', exc_info=True)
                try_close(conn_customer)
                try_close(conn_slaver)
                continue

    def _listen_slaver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.communicate_addr)
        sock.listen(10)
        _listening_sockets.append(sock)
        log.info("Listening for slavers: {}".format(
            fmt_addr(self.communicate_addr)))
        while True:
            conn, addr = sock.accept()
            self.slaver_pool.append({
                "addr_slaver": addr,
                "conn_slaver": conn,
            })
            log.info("Got slaver {} Total: {}".format(
                fmt_addr(addr), len(self.slaver_pool)
            ))

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.customer_listen_addr)
        sock.listen(20)
        _listening_sockets.append(sock)
        log.info("Listening for customers: {}".format(
            fmt_addr(self.customer_listen_addr)))
        while True:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, self.pending_customers.qsize() + 1
            ))

            # just put it into the queue,
            #   let _assign_slaver_daemon() do the else
            #   don't block this loop
            self.pending_customers.put((conn_customer, addr_customer))


def run_master(communicate_addr, customer_listen_addr, ssl=False):
    log.info("shootback {} running as master".format(version_info()))
    log.info("author: {}  site: {}".format(__author__, __website__))
    log.info("slaver from: {} customer from: {}".format(
        fmt_addr(communicate_addr), fmt_addr(customer_listen_addr)))

    Master(customer_listen_addr, communicate_addr, ssl=ssl).serve_forever()


def argparse_master():
    import argparse
    parser = argparse.ArgumentParser(
        description="""shootback (master) {ver}
A fast and reliable reverse TCP tunnel. (this is master)
Help access local-network service from Internet.
https://github.com/aploium/shootback""".format(ver=version_info()),
        epilog="""
Example1:
tunnel local ssh to public internet, assume master's ip is 1.2.3.4
  Master(this pc):                master.py -m 0.0.0.0:10000 -c 0.0.0.0:10022
  Slaver(another private pc):     slaver.py -m 1.2.3.4:10000 -t 127.0.0.1:22
  Customer(any internet user):    ssh 1.2.3.4 -p 10022
  the actual traffic is:  customer <--> master(1.2.3.4 this pc) <--> slaver(private network) <--> ssh(private network)

Example2:
Tunneling for www.example.com
  Master(this pc):                master.py -m 127.0.0.1:10000 -c 127.0.0.1:10080
  Slaver(this pc):                slaver.py -m 127.0.0.1:10000 -t example.com:80
  Customer(this pc):              curl -v -H "host: example.com" 127.0.0.1:10080

Tips: ANY service using TCP is shootback-able.  HTTP/FTP/Proxy/SSH/VNC/...
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-m", "--master", required=True,
                        metavar="host:port",
                        help="listening for slavers, usually an Public-Internet-IP. Slaver comes in here  eg: 2.3.3.3:10000")
    parser.add_argument("-c", "--customer", required=True,
                        metavar="host:port",
                        help="listening for customers, 3rd party program connects here  eg: 10.1.2.3:10022")
    parser.add_argument("-k", "--secretkey", default="shootback",
                        help="secretkey to identity master and slaver, should be set to the same value in both side")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="verbose output")
    parser.add_argument("-q", "--quiet", action="count", default=0,
                        help="quiet output, only display warning and errors, use two to disable output")
    parser.add_argument("-V", "--version", action="version", version="shootback {}-master".format(version_info()))
    parser.add_argument("--ttl", default=300, type=int, dest="SPARE_SLAVER_TTL",
                        help="standing-by slaver's TTL, default is 300. "
                             "In master side, this value affects heart-beat frequency. "
                             "Default value is optimized for most cases")
    parser.add_argument('--ssl', action='store_true', help='[experimental] try using ssl for data encryption. '
                                                           'It may be enabled by default in future version')

    return parser.parse_args()


def main_master():
    global SPARE_SLAVER_TTL

    args = argparse_master()

    if args.verbose and args.quiet:
        print("-v and -q should not appear together")
        exit(1)

    communicate_addr = split_host(args.master)
    customer_listen_addr = split_host(args.customer)

    set_secretkey(args.secretkey)

    SPARE_SLAVER_TTL = args.SPARE_SLAVER_TTL
    if args.quiet < 2:
        if args.verbose:
            level = logging.DEBUG
        elif args.quiet:
            level = logging.WARNING
        else:
            level = logging.INFO
        configure_logging(level)

    run_master(communicate_addr, customer_listen_addr, ssl=args.ssl)


if __name__ == '__main__':
    main_master()
