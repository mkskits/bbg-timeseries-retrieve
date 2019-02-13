# BBG time series retrieve scripts w/ BLPAPI  
Python scripts to retrieve timeseries from Bloomberg  
  
Retrieve intraday bars.  
Notes:  
1) All times are in GMT
2) Only one security can be specified
3) Only one event can be specified
  
Output stored in *.pkl file with under %security%.pkl

Options:  
  -h, --help            show this help message and exit  
  -a ipAddress, --ip=ipAddress  
                        server name or IP (default: localhost)  
  -p tcpPort            server port (default: 8194)  
  -s security           security (default: EURUSD BGN Curncy)  
  -e event              event (default: TRADE)  
  -b barInterval        bar interval (default: 15)  
  --sd=startDateTime    start date/time (default: 2019-01-11 15:30:00)  
  --ed=endDateTime      end date/time (default: 2019-02-13 17:30:00)  
  -g                    gapFillInitialBar  
  
Option Parameter Usage in the Command Prompt:  

python27 bbg_bar_retrieve_py27.py -s "EURCHF BGN Curncy" --ed "YYYY-MM-DD HH:MM:SS" --sd "YYYY-MM-DD HH:MM:SS"



