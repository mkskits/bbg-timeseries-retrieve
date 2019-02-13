# IntradayBarExample.py

import blpapi
import copy
import datetime
import time
from optparse import OptionParser, Option, OptionValueError
import pandas as pd

BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData")
OPEN = blpapi.Name("open")
HIGH = blpapi.Name("high")
LOW = blpapi.Name("low")
CLOSE = blpapi.Name("close")
VOLUME = blpapi.Name("volume")
NUM_EVENTS = blpapi.Name("numEvents")
TIME = blpapi.Name("time")
RESPONSE_ERROR = blpapi.Name("responseError")
SESSION_TERMINATED = blpapi.Name("SessionTerminated")
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")



def checkDateTime(option, opt, value):
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as ex:
        raise OptionValueError(
            "option {0}: invalid datetime value: {1} ({2})".format(
                opt, value, ex))


class ExampleOption(Option):
    TYPES = Option.TYPES + ("datetime",)
    TYPE_CHECKER = copy.copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["datetime"] = checkDateTime


def parseCmdLine():
    parser = OptionParser(description="Retrieve intraday bars.",
                          epilog="Notes: " +
                          "1) All times are in GMT. " +
                          "2) Only one security can be specified. " +
                          "3) Only one event can be specified.",
                          option_class=ExampleOption)
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    parser.add_option("-s",
                      dest="security",
                      help="security (default: %default)",
                      metavar="security",
                      default="EURUSD BGN Curncy")
    parser.add_option("-e",
                      dest="event",
                      help="event (default: %default)",
                      metavar="event",
                      default="TRADE")
    parser.add_option("-b",
                      dest="barInterval",
                      type="int",
                      help="bar interval (default: %default)",
                      metavar="barInterval",
                      default=15)
    parser.add_option("--sd",
                      dest="startDateTime",
                      type="datetime",
                      help="start date/time (default: %default)",
                      metavar="startDateTime",
                      default=datetime.datetime(2019, 01, 11, 15, 30, 0))
    parser.add_option("--ed",
                      dest="endDateTime",
                      type="datetime",
                      help="end date/time (default: %default)",
                      metavar="endDateTime",
                      # default=datetime.datetime(2019, 01, 29, 15, 35, 0))
                      default=round_time(datetime.datetime.now(), round_to=900))
    parser.add_option("-g",
                      dest="gapFillInitialBar",
                      help="gapFillInitialBar",
                      action="store_true",
                      default=False)

    (options, args) = parser.parse_args()
    global strSecurity
    strSecurity = options.security
    return options

def printErrorInfo(leadingStr, errorInfo):
    print "%s%s (%s)" % (leadingStr, errorInfo.getElementAsString(CATEGORY),
                         errorInfo.getElementAsString(MESSAGE))

def processMessage(msg):
    data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
    print "Datetime\t\tOpen\t\tHigh\t\tLow\t\tClose\t\tNumEvents\tVolume"

    n = 0
    # set up pandas
    columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'NumEvents', 'Volume']
    dftt = pd.DataFrame(columns=columns)
    pd_data = pd.DataFrame(columns=columns)

    for bar in data.values():
        time = bar.getElementAsDatetime(TIME)
        open = bar.getElementAsFloat(OPEN)
        high = bar.getElementAsFloat(HIGH)
        low = bar.getElementAsFloat(LOW)
        close = bar.getElementAsFloat(CLOSE)
        numEvents = bar.getElementAsInteger(NUM_EVENTS)
        volume = bar.getElementAsInteger(VOLUME)

        # newDF = newDF.append("%s\t\t%.3f\t\t%.3f\t\t%.3f\t\t%.3f\t\t%d\t\t%d" % \
        #     (time.strftime("%m/%d/%y %H:%M"), open, high, low, close,
        #      numEvents, volume))
        print "%s\t\t%.3f\t\t%.3f\t\t%.3f\t\t%.3f\t\t%d\t\t%d" % \
            (time.strftime("%m/%d/%y %H:%M"), open, high, low, close,
             numEvents, volume)
        dftt = pd.DataFrame([[(time.strftime("%m/%d/%y %H:%M")), open, high, low, close, numEvents, volume]], columns=columns)
        pd_data = pd_data.append(dftt, ignore_index=True)
        n = n+1

    # print datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print n, " rows"
    pd_data = pd_data.set_index('Datetime')
    pd_data.to_pickle("C:\\SRDEV\\D_Data\\" + strSecurity.replace(' ','_') + ".pkl")
    print "debugger"

def processResponseEvent(event):
    for msg in event:
        print msg
        if msg.hasElement(RESPONSE_ERROR):
            printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
            continue
        processMessage(msg)


def sendIntradayBarRequest(session, options):
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("IntradayBarRequest")

    # only one security/eventType per request
    request.set("security", options.security)
    request.set("eventType", options.event)
    request.set("interval", options.barInterval)

    # All times are in GMT
    if not options.startDateTime or not options.endDateTime:
        tradedOn = getPreviousTradingDate()
        if tradedOn:
            startTime = datetime.datetime.combine(tradedOn,
                                                  datetime.time(15, 30))
            request.set("startDateTime", startTime)
            endTime = datetime.datetime.combine(tradedOn,
                                                datetime.time(15, 35))
            request.set("endDateTime", endTime)
    else:
        if options.startDateTime and options.endDateTime:
            request.set("startDateTime", options.startDateTime)
            request.set("endDateTime", options.endDateTime)

    if options.gapFillInitialBar:
        request.set("gapFillInitialBar", True)

    print "Sending Request:", request
    session.sendRequest(request)


def eventLoop(session):
    done = False
    while not done:
        # nextEvent() method below is called with a timeout to let
        # the program catch Ctrl-C between arrivals of new events
        event = session.nextEvent(500)
        if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
            print "Processing Partial Response"
            processResponseEvent(event)
        elif event.eventType() == blpapi.Event.RESPONSE:
            print "Processing Response"
            processResponseEvent(event)
            done = True
        else:
            for msg in event:
                if event.eventType() == blpapi.Event.SESSION_STATUS:
                    if msg.messageType() == SESSION_TERMINATED:
                        done = True


def getPreviousTradingDate():
    tradedOn = datetime.date.today()

    while True:
        try:
            tradedOn -= datetime.timedelta(days=1)
        except OverflowError:
            return None

        if tradedOn.weekday() not in [5, 6]:
            return tradedOn

def round_time(dt=None, round_to=60):
   if dt == None:
       dt = datetime.datetime.now()
   seconds = (dt - dt.min).seconds
   rounding = (seconds+round_to/2) // round_to * round_to
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print "Connecting to %s:%s" % (options.host, options.port)
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print "Failed to start session."
        return

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print "Failed to open //blp/refdata"
            return

        sendIntradayBarRequest(session, options)

        # wait for events from session.
        eventLoop(session)

    finally:
        # Stop the session
        print "[" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "]: " + "data retrieve " + options.security + " executed"
        session.stop()
        print 'debugger'

if __name__ == "__main__":
    print "IntradayBarExample"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."


