import blpapi
from optparse import OptionParser
import pandas as pd
import pickle
import datetime as dt



def parseCmdLine():
    print('parser init')
    parser = OptionParser(description="Retrieve reference data.")
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

    parser.add_option("--security1", help="security 1", type="string", default='BBDXY Index')
    parser.add_option("--security2", help="security 2", type="string", default='')
    parser.add_option("--security3", help="security 3", type="string", default='')
    parser.add_option("--security4", help="security 3", type="string", default='')
    parser.add_option("--security5", help="security 3", type="string", default='')
    parser.add_option("--security6", help="security 3", type="string", default='')
    parser.add_option("--security7", help="security 3", type="string", default='')
    parser.add_option("--security8", help="security 3", type="string", default='')
    parser.add_option("--security9", help="security 3", type="string", default='')
    parser.add_option("--security10", help="security 3", type="string", default='')

    (options, args) = parser.parse_args()

    return options

def main():
    print('main init')
    options = parseCmdLine()

    print(options)
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

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        # request.getElement("securities").appendValue("IBM US Equity")
        if options.security1 != '':
            request.getElement("securities").appendValue(options.security1)
        if options.security2 != '':
            request.getElement("securities").appendValue(options.security2)
        if options.security3 != '':
            request.getElement("securities").appendValue(options.security3)
        if options.security4 != '':
            request.getElement("securities").appendValue(options.security4)
        if options.security5 != '':
            request.getElement("securities").appendValue(options.security5)
        if options.security6 != '':
            request.getElement("securities").appendValue(options.security6)
        if options.security7 != '':
            request.getElement("securities").appendValue(options.security7)
        if options.security8 != '':
            request.getElement("securities").appendValue(options.security8)
        if options.security9 != '':
            request.getElement("securities").appendValue(options.security9)
        if options.security10 != '':
            request.getElement("securities").appendValue(options.security10)

        request.getElement("fields").appendValue("PX_LAST")
        request.getElement("fields").appendValue("OPEN")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "DAILY")
        # dateformat YYYYMMDD
        request.set("startDate", "20140414")
        end_date = dt.datetime.today().strftime("%Y") + dt.datetime.today().strftime("%m") + dt.datetime.today().strftime("%d")
        request.set("endDate", end_date)
        request.set("maxDataPoints", 10000)

        print "Sending Request:", request
        # Send the request
        session.sendRequest(request)

        # Process received events
        i=0
        columns = ['DATE', 'LAST', 'SEC']
        dftt = pd.DataFrame(columns=columns)
        pd_data = pd.DataFrame(columns=columns)

        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            # print msg.getElement('securityData').getElementAsString('security')
            for msg in ev:
                # print 'placeholder'
                i = i + 1
                try:
                    print msg.getElement('securityData').getElementAsString('security')
                    for test in msg.getElement('securityData').getElement('fieldData').values():
                        del dftt
                        sec = msg.getElement('securityData').getElementAsString('security')
                        date = test.getElementAsDatetime('date')
                        print(date)
                        #px_open = test.getElementAsFloat('OPEN')
                        #print(px_open)
                        px_last = test.getElementAsFloat('PX_LAST')
                        print(px_last)
                        d = {'col1': [1, 2], 'col2': [3, 4]}
                        dftt = pd.DataFrame([[date, px_last, sec]], columns=columns)
                        pd_data = pd_data.append(dftt, ignore_index=True)
                except:
                    pass

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
    finally:
        # Stop the session
        session.stop()

        pd_data.to_pickle(r'C:\SRDEV\Data\BBG\eru0_daily.pickle')
        pd_data.to_csv(r'C:\SRDEV\Data\BBG\eru0_daily.csv', header = True, index = False)
        print('retrieve done')

if __name__ == "__main__":
    # print "SimpleHistoryExample"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."


