# SimpleHistoryExample.py

import blpapi
from optparse import OptionParser
import pandas as pd


def parseCmdLine():
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

    (options, args) = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    # print "Connecting to %s:%s" % (options.host, options.port)
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        # print "Failed to start session."
        return

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            # print "Failed to open //blp/refdata"
            return

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        # request.getElement("securities").appendValue("IBM US Equity")
        request.getElement("securities").appendValue("EURUSD BGN Curncy")
        request.getElement("securities").appendValue("USDCHF BGN Curncy")
        request.getElement("fields").appendValue("PX_LAST")
        request.getElement("fields").appendValue("OPEN")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "DAILY")
        request.set("startDate", "20180704")
        request.set("endDate", "20180705")
        request.set("maxDataPoints", 100)

        # print "Sending Request:", request
        # Send the request
        session.sendRequest(request)

        # Process received events
        i=0
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                print 'placeholder'
                i = i + 1
                print msg

                # msg.getElement('securityData').getElementAsString('security')
                # print(msg.getElement('securityData').getElementAsString('security'))

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        msg.getElement('securityData').getElementAsString('security')
        print msg.getElement('securityData').getElementAsString('security')

        columns = ['OPEN', 'LAST']
        df = pd.DataFrame(columns=columns)

        for test in msg.getElement('securityData').getElement('fieldData').values():
            date = test.getElementAsDatetime('date')
            print(date)
            px_open = test.getElementAsFloat('OPEN')
            print(px_open)
            px_last = test.getElementAsFloat('PX_LAST')
            print(px_last)
            # dft = pd.DataFrame([[px_open, px_last]], columns=columns, index = date)
            # dft.set_index(date, inplace=True, drop=False, append=False)

    finally:
        # Stop the session
        session.stop()

        # securityDataArray = msg.getElement('securityData')
        # for securityData in securityDataArray.values():
        #    fieldData = securityData.getElement("fieldData")
        #    for field in fieldData.elements():
        #         print(fieldData.elements())
        #         for n in range(field.getElementAsString()):
        #             print(field)

        # print(str)
        # print(msg)
        # print("%.2f" % fieldData.getElementAsFloat("FULL_REPO_PX_MID"))
        # print(fieldData.getElementAsString("PX_LAST"))
        # des = round(float(fieldData.getElementAsString("FULL_REPO_PX_MID")),2)
        # msg.getElement('securityData').getElementAsString('security')
        print('retrieve done')

if __name__ == "__main__":
    # print "SimpleHistoryExample"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."

__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""
