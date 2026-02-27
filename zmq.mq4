//+------------------------------------------------------------------+
//|                                   DWX_ZeroMQ_Server_Custom.mq4   |
//|                        Based on DWX ZeroMQ Server by Darwinex    |
//|                                      Optimized for Python Client |
//+------------------------------------------------------------------+
#property copyright "Copyright 2023, Custom Version"
#property link      "https://github.com/dingmaotu/mql-zmq"
#property version   "1.00"
#property strict

// Wajib: Library mql-zmq (https://github.com/dingmaotu/mql-zmq)
// Simpan di MQL4/Include/Zmq/
#include <Zmq/Zmq.mqh>

//--- Input Parameters
extern string  PROJECT_NAME      = "DWX_ZeroMQ_Server_Custom";
extern string  ZEROMQ_PROTOCOL   = "tcp";
extern string  HOSTNAME          = "*";
// Port disesuaikan dengan default live_atr_zmq.py
extern int     PULL_PORT         = 32768; // Menerima Command (Python PUSH -> MT4 PULL)
extern int     PUSH_PORT         = 32769; // Mengirim Reply   (Python PULL <- MT4 PUSH)
extern int     PUB_PORT          = 32770; // Publish Data     (Python SUB  <- MT4 PUB)
extern int     TIMER_MILLISECS   = 200;   // Interval timer
extern bool    VERBOSE           = true;

//--- Global Variables
Context context(PROJECT_NAME);
Socket pushSocket(context, ZMQ_PUSH); 
Socket pullSocket(context, ZMQ_PULL); 
Socket pubSocket(context, ZMQ_PUB);   

// Struktur untuk melacak instrumen yang disubscribe
class Instrument {
public:
   string symbol;
   int timeframe;
   datetime last_pub_time;
   
   Instrument() { symbol=""; timeframe=0; last_pub_time=0; }
   Instrument(string s, int tf) { symbol=s; timeframe=tf; last_pub_time=0; }
};

Instrument subscribed_instruments[];

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   // Setup ZMQ Sockets
   
   // Bind PULL Socket (Receive commands)
   if(!pullSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PULL_PORT))) {
      Print("Error binding PULL socket on port ", PULL_PORT);
      return(INIT_FAILED);
   }
   
   // Bind PUSH Socket (Send responses)
   if(!pushSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUSH_PORT))) {
      Print("Error binding PUSH socket on port ", PUSH_PORT);
      return(INIT_FAILED);
   }
   
   // Bind PUB Socket (Publish data)
   if(!pubSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUB_PORT))) {
      Print("Error binding PUB socket on port ", PUB_PORT);
      return(INIT_FAILED);
   }
   
   EventSetMillisecondTimer(TIMER_MILLISECS);
   Print("DWX Server Custom Started. Listening on PULL:", PULL_PORT, " PUSH:", PUSH_PORT, " PUB:", PUB_PORT);
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
   // Cek apakah ada instrumen yang perlu dipublish (Bar Close)
   for(int i=0; i<ArraySize(subscribed_instruments); i++) {
      PublishRates(i);
   }
}

//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
{
   // Proses pesan masuk dari Python
   ZmqMsg request;
   // Non-blocking receive
   if(pullSocket.recv(request, true)) {
      if(request.size() > 0) {
         string msg = request.getData();
         if(VERBOSE) Print("Received: ", msg);
         
         string reply = ProcessMessage(msg);
         
         // Kirim balasan
         ZmqMsg response(reply);
         pushSocket.send(response, true);
      }
   }
   
   // Cek publikasi di timer agar tetap terkirim meskipun market sedang sepi (tidak ada tick)
   for(int i=0; i<ArraySize(subscribed_instruments); i++) {
      PublishRates(i);
   }
}

//+------------------------------------------------------------------+
//| Helper: Process Incoming Message                                 |
//+------------------------------------------------------------------+
string ProcessMessage(string msg) {
   string parts[];
   string sep = "|"; 
   // Deteksi delimiter (; atau |)
   if(StringFind(msg, "|") >= 0) sep = "|";
   else if(StringFind(msg, ";") >= 0) sep = ";";
   
   StringSplit(msg, StringGetCharacter(sep, 0), parts);
   
   if(ArraySize(parts) == 0) return "ERROR_EMPTY";
   
   string cmd = parts[0];
   
   // 1. TRACK_RATES;SYMBOL;TIMEFRAME
   if(cmd == "TRACK_RATES") {
      if(ArraySize(parts) < 3) return "ERROR_ARGS";
      string sym = parts[1];
      int tf = (int)StringToInteger(parts[2]);
      
      int size = ArraySize(subscribed_instruments);
      ArrayResize(subscribed_instruments, size+1);
      subscribed_instruments[size] = Instrument(sym, tf);
      
      // Kirim data segera setelah subscribe berhasil
      PublishRates(size);
      
      return StringFormat("SUBSCRIBED %s %d", sym, tf);
   }
   
   // 2. TRADE|OPEN|TYPE|SYMBOL|LOTS|PRICE|SL|TP|COMMENT|MAGIC
   if(cmd == "TRADE") {
      if(ArraySize(parts) < 10) return "ERROR_ARGS_TRADE";
      
      string action = parts[1]; // OPEN
      int type = (int)StringToInteger(parts[2]); // 0=OP_BUY
      string symbol = parts[3];
      double lots = StringToDouble(parts[4]);
      double sl = StringToDouble(parts[6]);
      double tp = StringToDouble(parts[7]);
      string comment = parts[8];
      int magic = (int)StringToInteger(parts[9]);
      
      if(action == "OPEN") {
         double price = (type==0 ? MarketInfo(symbol,MODE_ASK) : MarketInfo(symbol,MODE_BID));
         int ticket = OrderSend(symbol, type, lots, price, 3, sl, tp, comment, magic, 0, clrNONE);
         if(ticket > 0) return StringFormat("OK_TICKET_%d", ticket);
         else return StringFormat("ERROR_%d", GetLastError());
      }
   }
   
   return "UNKNOWN_CMD";
}

//+------------------------------------------------------------------+
//| Helper: Publish Rates                                            |
//+------------------------------------------------------------------+
void PublishRates(int index) {
   Instrument inst = subscribed_instruments[index];
   
   // Ambil bar terakhir (closed bar, index 1)
   datetime bar_time = iTime(inst.symbol, inst.timeframe, 1);
   
   // Jika waktu bar closed > waktu terakhir publish, berarti ada bar baru
   if(bar_time > inst.last_pub_time) {
      double o = iOpen(inst.symbol, inst.timeframe, 1);
      double h = iHigh(inst.symbol, inst.timeframe, 1);
      double l = iLow(inst.symbol, inst.timeframe, 1);
      double c = iClose(inst.symbol, inst.timeframe, 1);
      long v = iVolume(inst.symbol, inst.timeframe, 1);
      
      // Format: TIME;OPEN;HIGH;LOW;CLOSE;VOLUME
      string payload = StringFormat("%d;%f;%f;%f;%f;%d", bar_time, o, h, l, c, v);
      
      // Topic: SYMBOL_M{TF}
      string topic = StringFormat("%s_M%d", inst.symbol, inst.timeframe);
      
      // Format ZMQ Message: "TOPIC:|:PAYLOAD"
      string msg_str = StringFormat("%s:|:%s", topic, payload);
      ZmqMsg msg(msg_str);
      
      if(pubSocket.send(msg, true)) {
         subscribed_instruments[index].last_pub_time = bar_time;
         if(VERBOSE) Print("Published: ", msg_str);
      }
   }
}//+------------------------------------------------------------------+
//|                                   DWX_ZeroMQ_Server_Custom.mq4   |
//|                        Based on DWX ZeroMQ Server by Darwinex    |
//|                                      Optimized for Python Client |
//+------------------------------------------------------------------+
#property copyright "Copyright 2023, Custom Version"
#property link      "https://github.com/dingmaotu/mql-zmq"
#property version   "1.00"
#property strict

// Wajib: Library mql-zmq (https://github.com/dingmaotu/mql-zmq)
// Simpan di MQL4/Include/Zmq/
#include <Zmq/Zmq.mqh>

//--- Input Parameters
extern string  PROJECT_NAME      = "DWX_ZeroMQ_Server_Custom";
extern string  ZEROMQ_PROTOCOL   = "tcp";
extern string  HOSTNAME          = "*";
// Port disesuaikan dengan default live_atr_zmq.py
extern int     PULL_PORT         = 32768; // Menerima Command (Python PUSH -> MT4 PULL)
extern int     PUSH_PORT         = 32769; // Mengirim Reply   (Python PULL <- MT4 PUSH)
extern int     PUB_PORT          = 32770; // Publish Data     (Python SUB  <- MT4 PUB)
extern int     TIMER_MILLISECS   = 200;   // Interval timer
extern bool    VERBOSE           = true;

//--- Global Variables
Context context(PROJECT_NAME);
Socket pushSocket(context, ZMQ_PUSH); 
Socket pullSocket(context, ZMQ_PULL); 
Socket pubSocket(context, ZMQ_PUB);   

// Struktur untuk melacak instrumen yang disubscribe
class Instrument {
public:
   string symbol;
   int timeframe;
   datetime last_pub_time;
   
   Instrument() { symbol=""; timeframe=0; last_pub_time=0; }
   Instrument(string s, int tf) { symbol=s; timeframe=tf; last_pub_time=0; }
};

Instrument subscribed_instruments[];

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   // Setup ZMQ Sockets
   
   // Bind PULL Socket (Receive commands)
   if(!pullSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PULL_PORT))) {
      Print("Error binding PULL socket on port ", PULL_PORT);
      return(INIT_FAILED);
   }
   
   // Bind PUSH Socket (Send responses)
   if(!pushSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUSH_PORT))) {
      Print("Error binding PUSH socket on port ", PUSH_PORT);
      return(INIT_FAILED);
   }
   
   // Bind PUB Socket (Publish data)
   if(!pubSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUB_PORT))) {
      Print("Error binding PUB socket on port ", PUB_PORT);
      return(INIT_FAILED);
   }
   
   EventSetMillisecondTimer(TIMER_MILLISECS);
   Print("DWX Server Custom Started. Listening on PULL:", PULL_PORT, " PUSH:", PUSH_PORT, " PUB:", PUB_PORT);
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
   // Cek apakah ada instrumen yang perlu dipublish (Bar Close)
   for(int i=0; i<ArraySize(subscribed_instruments); i++) {
      PublishRates(i);
   }
}

//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
{
   // Proses pesan masuk dari Python
   ZmqMsg request;
   // Non-blocking receive
   if(pullSocket.recv(request, true)) {
      if(request.size() > 0) {
         string msg = request.getData();
         if(VERBOSE) Print("Received: ", msg);
         
         string reply = ProcessMessage(msg);
         
         // Kirim balasan
         ZmqMsg response(reply);
         pushSocket.send(response, true);
      }
   }
   
   // Cek publikasi di timer agar tetap terkirim meskipun market sedang sepi (tidak ada tick)
   for(int i=0; i<ArraySize(subscribed_instruments); i++) {
      PublishRates(i);
   }
}

//+------------------------------------------------------------------+
//| Helper: Process Incoming Message                                 |
//+------------------------------------------------------------------+
string ProcessMessage(string msg) {
   string parts[];
   string sep = "|"; 
   // Deteksi delimiter (; atau |)
   if(StringFind(msg, "|") >= 0) sep = "|";
   else if(StringFind(msg, ";") >= 0) sep = ";";
   
   StringSplit(msg, StringGetCharacter(sep, 0), parts);
   
   if(ArraySize(parts) == 0) return "ERROR_EMPTY";
   
   string cmd = parts[0];
   
   // 1. TRACK_RATES;SYMBOL;TIMEFRAME
   if(cmd == "TRACK_RATES") {
      if(ArraySize(parts) < 3) return "ERROR_ARGS";
      string sym = parts[1];
      int tf = (int)StringToInteger(parts[2]);
      
      int size = ArraySize(subscribed_instruments);
      ArrayResize(subscribed_instruments, size+1);
      subscribed_instruments[size] = Instrument(sym, tf);
      
      // Kirim data segera setelah subscribe berhasil
      PublishRates(size);
      
      return StringFormat("SUBSCRIBED %s %d", sym, tf);
   }
   
   // 2. TRADE|OPEN|TYPE|SYMBOL|LOTS|PRICE|SL|TP|COMMENT|MAGIC
   if(cmd == "TRADE") {
      if(ArraySize(parts) < 10) return "ERROR_ARGS_TRADE";
      
      string action = parts[1]; // OPEN
      int type = (int)StringToInteger(parts[2]); // 0=OP_BUY
      string symbol = parts[3];
      double lots = StringToDouble(parts[4]);
      double sl = StringToDouble(parts[6]);
      double tp = StringToDouble(parts[7]);
      string comment = parts[8];
      int magic = (int)StringToInteger(parts[9]);
      
      if(action == "OPEN") {
         double price = (type==0 ? MarketInfo(symbol,MODE_ASK) : MarketInfo(symbol,MODE_BID));
         int ticket = OrderSend(symbol, type, lots, price, 3, sl, tp, comment, magic, 0, clrNONE);
         if(ticket > 0) return StringFormat("OK_TICKET_%d", ticket);
         else return StringFormat("ERROR_%d", GetLastError());
      }
   }
   
   return "UNKNOWN_CMD";
}

//+------------------------------------------------------------------+
//| Helper: Publish Rates                                            |
//+------------------------------------------------------------------+
void PublishRates(int index) {
   Instrument inst = subscribed_instruments[index];
   
   // Ambil bar terakhir (closed bar, index 1)
   datetime bar_time = iTime(inst.symbol, inst.timeframe, 1);
   
   // Jika waktu bar closed > waktu terakhir publish, berarti ada bar baru
   if(bar_time > inst.last_pub_time) {
      double o = iOpen(inst.symbol, inst.timeframe, 1);
      double h = iHigh(inst.symbol, inst.timeframe, 1);
      double l = iLow(inst.symbol, inst.timeframe, 1);
      double c = iClose(inst.symbol, inst.timeframe, 1);
      long v = iVolume(inst.symbol, inst.timeframe, 1);
      
      // Format: TIME;OPEN;HIGH;LOW;CLOSE;VOLUME
      string payload = StringFormat("%d;%f;%f;%f;%f;%d", bar_time, o, h, l, c, v);
      
      // Topic: SYMBOL_M{TF}
      string topic = StringFormat("%s_M%d", inst.symbol, inst.timeframe);
      
      // Format ZMQ Message: "TOPIC:|:PAYLOAD"
      string msg_str = StringFormat("%s:|:%s", topic, payload);
      ZmqMsg msg(msg_str);
      
      if(pubSocket.send(msg, true)) {
         subscribed_instruments[index].last_pub_time = bar_time;
         if(VERBOSE) Print("Published: ", msg_str);
      }
   }
}