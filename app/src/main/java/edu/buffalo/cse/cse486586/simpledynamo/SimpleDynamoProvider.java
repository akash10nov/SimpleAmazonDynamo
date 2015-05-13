package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

public class SimpleDynamoProvider extends ContentProvider {

    String myport;
    static final int MESSAGE_KEY=0;
    static final int MESSAGE_VALUE=1;
    static final int MESSAGE_TYPE=2;
    static final int MSG_TEXT=3;
    static final int MYPORT=4;
    static final int SENDPORT=5;
    static final int CO=6;
    String succesor=null;
    Object lock;
    int q_flag=0;
    int q_all_counter=0;
    int q_all_flag=0;
    ContentValues cv_solo;
    String R_key;
    String R_value;
    static int s_port = 10000;
    // 5562-5556-5554-5558-5560
    String Port[];
    String hashgen[];
    String predecessor=null;
    String Final_String;
    //static final int hash=8;
    HashMap<String ,Object > keylock;
    private SQLiteDatabase database;
    static String database_name="MYDB";
    static String msg_table="MSGTABLE";
    static String node_table="NODETABLE";
    static final String  create_table="CREATE TABLE " + msg_table + " ( key STRING PRIMARY KEY," + " value STRING , coordinator STRING)";
    String TAG ="SimpleDynamo";
    Uri mUri;
    ArrayList<String> nodes;
    HashMap<String, String> resultAll = new HashMap<>();
    HashMap<String, Integer > flag_query;



    public String[] dissectMessage(String s)
    {
        return s.split("#");
    }

    public String prepareMessage(String[] str){
        StringBuilder sb = new StringBuilder("");

        for(String s: str){
            sb.append(s);
            sb.append("#");
        }
//        Log.i(TAG,sb.toString());
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
    public String[] dissectMessage_final(String s){
        return s.split("--");
    }
    // 5562-5556-5554-5558-5560
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String Insert_hash = null;
        try {
            Insert_hash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int flag=0;
        int i=0;
        for(i=0;i<5;i++)
        {
            if((Insert_hash.compareTo(hashgen[i])<=0))
            {
                flag=1;
                break;
            }
        }
        if(flag==0)
            i=0;
        if(Port[i].equals(myport))
        {
            Cursor position =null;
            position= database.rawQuery("SELECT * from MSGTABLE where key = ?",new String[]{selection});
            if(position!= null && position.getCount()>0)
            {
                database.delete(msg_table,"key='"+selection+"'",null);
            }
            String[] MsgInsert=new String[7];
            MsgInsert[SENDPORT]=Port[(i+1)%5];
            MsgInsert[MESSAGE_KEY]=selection;
            // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert[CO]=Port[i];
            MsgInsert[MESSAGE_TYPE]="Delete";
            String MSGtoSEND=prepareMessage(MsgInsert);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
            String[] MsgInsert2=new String[7];
            MsgInsert2[SENDPORT]=Port[(i+2)%5];
            MsgInsert2[MESSAGE_KEY]=selection;
            // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert2[CO]=Port[i];
            MsgInsert2[MESSAGE_TYPE]="Delete";
            String MSGtoSEND1=prepareMessage(MsgInsert2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);
        }
        else
        {
            String[] MsgInsert=new String[7];
            MsgInsert[SENDPORT]=Port[i];
            MsgInsert[MESSAGE_KEY]=selection;
            // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert[MESSAGE_TYPE]="Delete";
            MsgInsert[CO]=Port[i];
            String MSGtoSEND=prepareMessage(MsgInsert);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
            String[] MsgInsert2=new String[7];
            MsgInsert2[SENDPORT]=Port[(i+1)%5];
            MsgInsert2[MESSAGE_KEY]=selection;
            // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert2[CO]=Port[i];
            MsgInsert2[MESSAGE_TYPE]="Delete";
            String MSGtoSEND2=prepareMessage(MsgInsert2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND2, null);
            String[] MsgInsert3=new String[7];
            MsgInsert3[SENDPORT]=Port[(i+2)%5];
            MsgInsert3[MESSAGE_KEY]=selection;
            // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert3[CO]=Port[i];
            MsgInsert3[MESSAGE_TYPE]="Delete";
            String MSGtoSEND1=prepareMessage(MsgInsert3);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String Insert_hash = null;
        try {
            Insert_hash = genHash((String)values.get("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int flag=0;
        int i=0;
        for(i=0;i<5;i++)
        {
            if((Insert_hash.compareTo(hashgen[i])<=0))
            {
                flag=1;
                break;
            }
        }
        if(flag==0)
            i=0;
        if(Port[i].equals(myport))
        {
            String value = (String)values.get("value");
            String key = (String)values.get("key");

            values.put("coordinator", Port[i]);
            Log.i(TAG, "local insert: " + key +" value:"+value);

            database.insertWithOnConflict(msg_table,"",values,SQLiteDatabase.CONFLICT_REPLACE);
            String[] MsgInsert=new String[7];
            MsgInsert[SENDPORT]=Port[(i+1)%5];
            MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert[CO]=Port[i];
            MsgInsert[MESSAGE_TYPE]="Insert";
            String MSGtoSEND=prepareMessage(MsgInsert);
            Log.i(TAG, "fwded to neighbor on p[]=myport: " + key +" value:"+value+"to:"+Port[(i+1)%5]);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
            String[] MsgInsert1=new String[7];
            MsgInsert1[SENDPORT]=Port[(i+2)%5];
            MsgInsert1[MESSAGE_KEY]=(String)values.get("key");
            MsgInsert1[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert1[CO]=Port[i];
            MsgInsert1[MESSAGE_TYPE]="Insert";
            String MSGtoSEND1=prepareMessage(MsgInsert1);
            Log.i(TAG, "fwded to neighbor on p[]=myport: " + key +" value:"+value+"to:"+Port[(i+2)%5]);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);
        }
        else
        {
            String value = (String)values.get("value");
            String key = (String)values.get("key");

            String[] MsgInsert=new String[7];
            Log.i(TAG, "fwded : " + key +" value:"+value+"to:"+Port[i]);
            MsgInsert[SENDPORT]=Port[i];
            MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert[MESSAGE_TYPE]="Insert";
            MsgInsert[CO]=Port[i];
            String MSGtoSEND=prepareMessage(MsgInsert);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
            String[] MsgInsert1=new String[7];
            MsgInsert1[SENDPORT]=Port[(i+1)%5];
            MsgInsert1[MESSAGE_KEY]=(String)values.get("key");
            MsgInsert1[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert1[CO]=Port[i];
            Log.i(TAG, "fwded : " + key +" value:"+value+"to:"+Port[(i+1)%5]);
            MsgInsert1[MESSAGE_TYPE]="Insert";
            String MSGtoSEND2=prepareMessage(MsgInsert1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND2, null);
            String[] MsgInsert2=new String[7];
            MsgInsert2[SENDPORT]=Port[(i+2)%5];
            MsgInsert2[MESSAGE_KEY]=(String)values.get("key");
            MsgInsert2[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert2[CO]=Port[i];
            MsgInsert2[MESSAGE_TYPE]="Insert";
            Log.i(TAG, "fwded : " + key +" value:"+value+"to:"+Port[(i+2)%5]);
            String MSGtoSEND1=prepareMessage(MsgInsert2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);

        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myport = String.valueOf((Integer.parseInt(portStr) * 2));
        int q_flag=0;
        q_all_counter=0;
        q_all_flag=0;
        keylock=new HashMap<>();
        flag_query=new HashMap<>();
        Final_String=null;
        Port=new String[]{"11124","11112","11108","11116","11120"};
        hashgen=new String[]{"177ccecaec32c54b82d5aaafc18a2dadb753e3b1","208f7f72b198dadd244e61801abe1ec3a4857bc9","33d6357cfaaf0f72991b0ecd8c56da066613c089","abf0fd8db03e5ecb199a9b82929e9db79b909643","c25ddd596aa7c81fa12378fa725f706d54325d12"};

        lock=new Object();
//        try{
//            hashgen= genHash(Integer.toString(Integer.parseInt(myport) / 2));
//        }
//        catch(NoSuchAlgorithmException e)
//        {
//            // put something if needed later on.
//        }
        Context context = getContext();
        mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
        MainDatabaseHelper dbHelper = new MainDatabaseHelper(context);
        database = dbHelper.getWritableDatabase();
        Log.e(TAG, "star ");
        try
        {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,new ServerSocket(s_port));
        }
        catch (IOException e){
        }
        int i=0;
        for(i=0;i<5;i++)
        {
            if(myport.equals(Port[i]))
                break;
        }
        String[] MsgInsert=new String[7];
        MsgInsert[SENDPORT]=Port[(i+1)%5];
        MsgInsert[MYPORT]=myport;
        //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
        //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
        MsgInsert[CO]=myport;
        MsgInsert[MESSAGE_TYPE]="fill_cordinator";
        String MSGtoSEND=prepareMessage(MsgInsert);
        Log.i(TAG, "req_fill_co-or: " + MSGtoSEND);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
        String[] MsgInsert2=new String[7];
        MsgInsert2[SENDPORT]=Port[(i+2)%5];
        MsgInsert2[MYPORT]=myport;
        //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
        //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
        MsgInsert2[CO]=myport;
        MsgInsert2[MESSAGE_TYPE]="fill_cordinator";
        String MSGtoSEND2=prepareMessage(MsgInsert2);
        Log.i(TAG, "req_fill_co-or: " + MSGtoSEND2);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND2, null);
        if(i==0)
        {
            String[] MsgInsert3=new String[7];
            MsgInsert3[SENDPORT]=Port[4];
            MsgInsert3[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert3[CO]=Port[4];
            MsgInsert3[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND3=prepareMessage(MsgInsert3);
            Log.i(TAG, "req_fill_replica : " + MSGtoSEND3);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND3, null);
            String[] MsgInsert4=new String[7];
            MsgInsert4[SENDPORT]=Port[3];
            MsgInsert4[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert4[CO]=Port[3];
            MsgInsert4[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND4=prepareMessage(MsgInsert4);
            Log.i(TAG, "req_fill_replica : " + MSGtoSEND4);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND4, null);
        }

        if(i==1)
        {

            String[] MsgInsert3=new String[7];
            MsgInsert3[SENDPORT]=Port[4];
            MsgInsert3[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert3[CO]=Port[4];
            MsgInsert3[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND3=prepareMessage(MsgInsert3);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND3, null);
            String[] MsgInsert4=new String[7];
            MsgInsert4[SENDPORT]=Port[0];
            MsgInsert4[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert4[CO]=Port[0];
            MsgInsert4[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND4=prepareMessage(MsgInsert4);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND4, null);
        }
        if(i>1)
        {

            String[] MsgInsert3=new String[7];
            MsgInsert3[SENDPORT]=Port[i-1];
            MsgInsert3[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert3[CO]=Port[i-1];
            MsgInsert3[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND3=prepareMessage(MsgInsert3);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND3, null);
            String[] MsgInsert4=new String[7];
            MsgInsert4[SENDPORT]=Port[i-2];
            MsgInsert4[MYPORT]=myport;
            //MsgInsert[MESSAGE_KEY]=(String)values.get("key");
            //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
            MsgInsert4[CO]=Port[i-2];
            MsgInsert4[MESSAGE_TYPE]="fill_replica";
            String MSGtoSEND4=prepareMessage(MsgInsert4);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND4, null);
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        String sel=selection;
        Cursor position = null;
        switch (sel)
        {
            case "\"*\"":

                position = database.rawQuery("SELECT key,value FROM MSGTABLE",null);


                if(position!=null && position.getCount()>0){
                    position.moveToFirst();
                    for(int i = 0 ; i < position.getCount() ; i++){
                        resultAll.put(position.getString(0),position.getString(1));
                        position.moveToNext();
                    }
                }
                for(int i=0;i<5;i++)
                {
                    if(Port[i].equals(myport))
                    {

                    }
                    else
                    {
                        String[] MsgInsert=new String[7];
                        MsgInsert[SENDPORT]=Port[i];
                        //MsgInsert[MESSAGE_KEY]=selection;
                        // MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                        //MsgInsert[CO]=Port[(i+1)%5];
                        MsgInsert[MESSAGE_TYPE]="reply_all";
                        MsgInsert[MYPORT]=myport;
                        String MSGtoSEND=prepareMessage(MsgInsert);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
                    }
                }
                q_all_flag=1;
                synchronized (lock){
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                q_all_flag=0;
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
                for(Map.Entry<String ,String > entry : resultAll.entrySet()){
                    matrixCursor.addRow(new Object[]{entry.getKey(),entry.getValue()});
                    Log.i(TAG,entry.getKey()+" "+entry.getValue());
                }
                position = matrixCursor;
                position.moveToFirst();
                resultAll=null;

                break;
            case "\"@\"":
                position=null;
                position= database.rawQuery("SELECT key,value FROM MSGTABLE",null);
                break;
            default:
                String Insert_hash = null;
                try {
                    Insert_hash = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                int flag=0;
                int i=0;
                for(i=0;i<5;i++)
                {
                    if((Insert_hash.compareTo(hashgen[i])<=0))
                    {
                        flag=1;
                        break;
                    }
                }
                if(flag==0)
                    i=0;
                if(Port[i].equals(myport))
                {
                    Log.i(TAG, "queried : " + selection +" value:"+ Insert_hash+"on:"+myport);
                    position=database.rawQuery("SELECT key,value from MSGTABLE where key = ?",new String[]{selection});
                    return position;
                    // Log.i(TAG, "queried : " + selection +" value:"+ Insert_hash+"on:"+myport);

                }
                else
                {


                    /*
                    String[] MsgInsert2=new String[7];

                    MsgInsert2[SENDPORT]=Port[(i+2)%5];
                    MsgInsert2[MYPORT]=myport;
                    MsgInsert2[MESSAGE_KEY]=selection;
                    //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                    MsgInsert2[MESSAGE_TYPE]="solo_q";
                    //MsgInsert[CO]=Port[i];
                    String MSGtoSEND2=prepareMessage(MsgInsert2);
                    q_flag=1;
                    */

                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND2, null);
                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);

                    String[] MsgInsert=new String[7];
                    Log.i(TAG, "queried fwd : " + selection +" value:"+ Insert_hash+"on:"+Port[i]+"neightbor"+Port[(i+1)%5]+ Port[(i+2)%5]);
                    MsgInsert[SENDPORT]=Port[i];
                    MsgInsert[MYPORT]=myport;
                    MsgInsert[MESSAGE_KEY]=selection;
                    //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                    MsgInsert[MESSAGE_TYPE]="solo_q";
                    //MsgInsert[CO]=Port[i];
                    String MSGtoSEND=prepareMessage(MsgInsert);
                    String[] MsgInsert2=new String[7];
                    MsgInsert2[SENDPORT]=Port[(i+2)%5];
                    MsgInsert2[MYPORT]=myport;
                    MsgInsert2[MESSAGE_KEY]=selection;
                    //MsgInsert[MESSAGE_VALUE]=(String)values.get("value");
                    MsgInsert2[MESSAGE_TYPE]="solo_q";
                    //MsgInsert[CO]=Port[i];
                    String MSGtoSEND2=prepareMessage(MsgInsert2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND, null);
                    flag_query.put(selection,0);
                    keylock.put(selection, new Object());
                    synchronized (keylock.get(selection)) {
                        try {
                            keylock.get(selection).wait(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if(flag_query.get(selection)!=1) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND2, null);
                        synchronized (keylock.get(selection)) {
                            try {
                                keylock.get(selection).wait();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    HashMap<String, String> result_solo = new HashMap<>();
                    String Str_ans[] = dissectMessage_final(Final_String);
                    Log.i(TAG, Str_ans[0] + ":key  value:" + Str_ans[1]);
                    result_solo.put(Str_ans[0], Str_ans[1]);
                    MatrixCursor matrixCursor2 = new MatrixCursor(new String[]{"key", "value"});

                    matrixCursor2.addRow(new Object[]{Str_ans[0], Str_ans[1]});
                    Log.i(TAG, Str_ans[0] + " THIS IS WHAT I AM GETIING" + Str_ans[1]);
                    Cursor pos=null;
                    pos= matrixCursor2;
                    pos.moveToFirst();
                    return pos;
                    //position.moveToFirst();


                }
        }
        return position;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgToDeliver = dissectMessage(msgs[0]);
            Socket socket;
            try{
                ObjectOutputStream PW;
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToDeliver[SENDPORT]));
                String msgToSend = msgs[0];
                PW = new ObjectOutputStream(socket.getOutputStream());
                PW.writeObject(msgToSend);
                PW.flush();
                PW.close();
                socket.close();
            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");

            }
            return null;

        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ObjectInputStream ISR=null;
            Socket socketonReceive=null;
            while (true) {
                try {
                    socketonReceive = serverSocket.accept();
                    ISR = new ObjectInputStream(socketonReceive.getInputStream());
                    String msg=(String)ISR.readObject();
                    onProgressUpdate(msg);
                    ISR.close();
                    socketonReceive.close();

                } catch (IOException ex) {
                    Log.e(TAG, "IO Exception in Server Side");

                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    try{
                        socketonReceive.close();
                        ISR.close();
                    }
                    catch (IOException e)
                    {
                        Log.e(TAG,"IO exception while closing the server side.");
                    }

                }


            }
        }
        protected void onProgressUpdate(final String...strings)
        {
            final String strReceived = strings[0];
            Log.i(TAG, "Received: " + strReceived);
            String[] message = dissectMessage(strReceived);
            Cursor position=null;
            switch(message[MESSAGE_TYPE])
            {
                case "Insert":
                    ContentValues cv = new ContentValues();
                    cv.put("key",message[MESSAGE_KEY]);
                    cv.put("value",message[MESSAGE_VALUE]);
                    cv.put("coordinator",message[CO]);
                    database.insertWithOnConflict(msg_table, "", cv, SQLiteDatabase.CONFLICT_REPLACE);
                    //insert(mUri, cv);
                    break;
                case "reply_all":
                    position=null;
                    position = database.rawQuery("SELECT * FROM MSGTABLE",null);
                    message[MESSAGE_TYPE]="received";
                    message[SENDPORT]=message[MYPORT];
                    //MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);

                    if(position!=null && position.getCount()>0){
                        position.moveToFirst();
                        for(int i = 0 ; i < position.getCount() ; i++){
                            if(i==0)
                                message[MSG_TEXT]=position.getString(0)+"--"+position.getString(1);
                            else
                                message[MSG_TEXT]=message[MSG_TEXT]+"--"+position.getString(0)+"--"+position.getString(1);

                            position.moveToNext();
                        }
                    }
                    String MSGtoSEND2=prepareMessage(message);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND2, null);
                    break;
                case "received":
                    if(q_all_flag==0)
                    {}
                    else if(q_all_flag==1){
                        q_all_counter++;
                        String[] Str_Ans=dissectMessage_final(message[MSG_TEXT]);
                        for(int i=0;i<Str_Ans.length;i=i+2)
                        {
                            resultAll.put(Str_Ans[i],Str_Ans[i+1]);
                        }
                        if(q_all_counter==3) {
                            q_all_flag=0;
                            synchronized (lock) {
                                lock.notify();
                            }
                        }

                    }
                    break;
                case "Delete":
                    position=null;
                    position = database.rawQuery("SELECT key,value from MSGTABLE where key = ?",new String[]{message[MESSAGE_KEY]});
                    if(position!= null && position.getCount()>0)
                    {
                        database.delete(msg_table,"key='"+message[MESSAGE_KEY]+"'",null);
                    }
                    break;
                case "solo_q":
                    position=null;
                    position = database.rawQuery("SELECT key,value from MSGTABLE where key = ?",new String[]{message[MESSAGE_KEY]});
                    message[MESSAGE_TYPE]="solo_received";
                    message[SENDPORT]=message[MYPORT];
                    //MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);

                    if(position!=null && position.getCount()>0){
                        position.moveToFirst();

                        message[MSG_TEXT]=position.getString(0)+"--"+position.getString(1);

                    }
                    String MSGtoSEND1=prepareMessage(message);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,MSGtoSEND1, null);

                    break;
                case "solo_received":
                    //Log.i(TAG,"solo received"+message[MSG_TEXT]);
                    //String[] Str_Ans=dissectMessage_final(message[MSG_TEXT]);
                    Final_String=message[MSG_TEXT];
                    Log.i(TAG,"solo received"+Final_String);
                    String key = dissectMessage_final(Final_String)[0];
                        synchronized (keylock.get(key)) {
                            Log.i(TAG, "lock:solo received" + Final_String);
                            flag_query.put(key, 1);
                            keylock.get(key).notify();
                        }


                    break;
                case "fill_cordinator":
                    position=null;
                    position = database.rawQuery("SELECT * FROM MSGTABLE",null);
                    //position = database.rawQuery("SELECT key,value from MSGTABLE where coordinator = ?",new String[]{message[MYPORT]});
                    message[MESSAGE_TYPE]="refilled";
                    message[SENDPORT]=message[MYPORT];
                    //MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
                    int flg1=0;
                    if(position!=null && position.getCount()>0){
                        position.moveToFirst();
                        for(int i = 0 ; i < position.getCount() ; i++){
                            if(position.getString(2)!=null) {
                                if (flg1 == 0 && position.getString(2).equals(message[SENDPORT])) {
                                    message[MSG_TEXT] = position.getString(0) + "--" + position.getString(1);
                                    flg1 = 1;
                                } else if (position.getString(2).equals(message[SENDPORT]))
                                    message[MSG_TEXT] = message[MSG_TEXT] + "--" + position.getString(0) + "--" + position.getString(1);
                            }

                            position.moveToNext();
                        }
                        Log.i(TAG, "fill_coordi: " + message[CO]);

                        String MSGtoSEND23=prepareMessage(message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND23, null);
                    }


                    break;
                case "refilled":
                    String[] Str_Ans1=dissectMessage_final(message[MSG_TEXT]);
                    Log.i(TAG, "refilled: " + message[CO]);
                    ContentValues values = new ContentValues();
                    for(int i=0;i<(Str_Ans1.length-1);i=i+2)
                    {
                        values.put("key",Str_Ans1[i]);
                        Log.e(TAG, "refilled keys: " + Str_Ans1[i]);
                        values.put("value", Str_Ans1[i + 1]);
                        Log.i(TAG, "refilled values: " + Str_Ans1[i + 1]);
                        values.put("coordinator", myport);
                        database.insertWithOnConflict(msg_table, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                    //database.insertWithOnConflict(msg_table,"",values,SQLiteDatabase.CONFLICT_REPLACE);
                    break;
                case "refilled_replica":
                    String[] Str_Ans2=dissectMessage_final(message[MSG_TEXT]);
                    ContentValues values1 = new ContentValues();
                    for(int i=0;i<(Str_Ans2.length-1);i=i+2)
                    {
                        values1.put("key",Str_Ans2[i]);
                        values1.put("value",Str_Ans2[i+1]);
                        values1.put("coordinator",message[CO]);
                        Log.i(TAG, "fill_replica : " + message[CO] + "key: " + Str_Ans2[i] + "value: " + Str_Ans2[i + 1]);
                        database.insertWithOnConflict(msg_table, "", values1, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                    // database.insertWithOnConflict(msg_table,"",values1,SQLiteDatabase.CONFLICT_REPLACE);
                    break;
                case "fill_replica":
                    position=null;
                    position = database.rawQuery("SELECT * FROM MSGTABLE",null);
                    message[MESSAGE_TYPE]="refilled_replica";
                    message[SENDPORT]=message[MYPORT];
                    //MESSAGE[SENDPORT]=Integer.toString(Integer.parseInt(PortList.get(succesor))*2);
                    int flg2=0;
                    Log.i(TAG, "fill_replica : " + message[CO]);
                    Log.i(TAG, "fill_replica GET COUNT: " + position.getCount());
                    if(position!=null && position.getCount()>0){
                        position.moveToFirst();
                        for(int i = 0 ; i < position.getCount() ; i++){
                            Log.i(TAG, "fill_replica_counter : " + i +"co"+message[CO]);
                            if(position.getString(2)!=null) {
                                if (flg2 == 0 && position.getString(2).equals(message[CO])) {
                                    ContentValues cv1=new ContentValues();
                                    cv1.put("key",position.getString(0));
                                    cv1.put("value",position.getString(1));
                                    cv1.put("coordinator",position.getString(2));
                                    message[MSG_TEXT] = position.getString(0) + "--" + position.getString(1);
                                    database.insertWithOnConflict(msg_table,"",cv1,SQLiteDatabase.CONFLICT_REPLACE);
                                    Log.i(TAG, "fill_replica : " + message[CO] + "key: " + position.getString(0) + "value: " + position.getString(1));
                                    flg2 = 1;
                                } else if (position.getString(2).equals(message[CO])) {

                                    ContentValues cv12=new ContentValues();
                                    cv12.put("key",position.getString(0));
                                    cv12.put("value",position.getString(1));
                                    cv12.put("coordinator",position.getString(2));
                                    database.insertWithOnConflict(msg_table,"",cv12,SQLiteDatabase.CONFLICT_REPLACE);
                                    message[MSG_TEXT] = message[MSG_TEXT] + "--" + position.getString(0) + "--" + position.getString(1);
                                    Log.i(TAG, "fill_replica : " + message[CO] + "key: " + position.getString(0) + "value: " + position.getString(1));

                                }
                            }
                            position.moveToNext();
                        }
                        String MSGtoSEND23=prepareMessage(message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MSGtoSEND23, null);
                    }
                    break;
            }

        }
    }


    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        /*
         * Instantiates an open helper for the provider's SQLite data repository
         * Do not do database creation and upgrade here.
         */
        MainDatabaseHelper(Context context) {
            super(context, database_name, null, 3);
        }
        public void onCreate(SQLiteDatabase database) {

            // Creates the main table
            database.execSQL(create_table);
        }

        @Override
        public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i2) {

        }
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
