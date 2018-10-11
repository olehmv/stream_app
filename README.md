This app use apache spark structured streaming to stream data from source to sinks
   
   Usage : 
    
   Parameterize and pass to program arguments parameter.xml  

    <parameter>
      <source sourcetable="name of table you will use in sql files query" path="from where to read" format="format of source">
        <watermark timeInterval="how long wait for late data" columnname="source column name to check for delay, must be timestamp"/>
        <option value="value to apply for source" key="config spark key"/>
      </source>
      <transformcolumnname pattern="what time stamp pattern to follow when parse this column">
        <column to="to new column name" from="from old column name"/>
      </transformcolumnname>
      <sink outputMode="how to output to sink (append, complete, update)" format="format of sink">
        <executequery sqlfile="location of sql file, where query use table name from sourcetable attribute in source xml elem">
          <checkpoint value="checkpoin location required for this query" key="config spark key"/>
        </executequery>
       <option value="value to apply for sink" key="config spark key"/>
      </sink>
    </parameter>

   Example from test/resources/parameter.xml:
   

    <parameter>
      <source sourcetable="fire_calls" path="src/test/resources/source" format="csv">
        <watermark timeInterval="1 day" columnname="CallDateTs"/>
        <option value="true" key="header"/>
        <option value="true" key="inferSchema"/>
      </source>
      <transformcolumnname pattern="MM/dd/yyyy">
        <column to="CallDateTs" from="Call Date"/>
        <column to="WatchDateTs" from="Watch Date"/>
      </transformcolumnname>
      <transformcolumnname pattern="MM/dd/yyyy hh:mm:ss aa">
        <column to="EntryDtTs" from="Entry DtTm"/>
        <column to="ReceivedDtTs" from="Received DtTm"/>
        <column to="DispatchDtTs" from="Dispatch DtTm"/>
        <column to="ResponseDtTs" from="Response DtTm"/>
        <column to="OnSceneDtTs" from="On Scene DtTm"/>
        <column to="TransportDtTs" from="Transport DtTm"/>
        <column to="HospitalDtTs" from="Hospital DtTm"/>
        <column to="AvailableDtTs" from="Available DtTm"/>
      </transformcolumnname>
      <sink outputMode="append" format="parquet">
        <executequery sqlfile="src/test/resources/sql/streamoperation/group_by_day_stream.sql">
          <checkpoint value="src/test/resources/checkpoint_sink1" key="checkpointLocation"/>
        </executequery>
        <option value="src/test/resources/sink/group_by_day_stream" key="path"/>
      </sink>
      <sink outputMode="append" format="parquet">
        <executequery sqlfile="src/test/resources/sql/streamoperation/group_by_month_stream.sql">
          <checkpoint value="src/test/resources/checkpoint_sink2" key="checkpointLocation"/>
        </executequery>
        <option value="src/test/resources/sink/group_by_month_stream" key="path"/>
      </sink>
      <sink outputMode="append" format="parquet">
        <executequery sqlfile="src/test/resources/sql/streamoperation/group_by_year_stream.sql">
          <checkpoint value="src/test/resources/checkpoint_sink3" key="checkpointLocation"/>
        </executequery>
        <option value="src/test/resources/sink/group_by_year_stream" key="path"/>
      </sink>
    </parameter>
    
For test used dataset from https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3

Use test/resources/databriks_notebook_fire_calls_dataset.html notebook to prototype dataframe operation
   
   
   
  
              
    
    
    
    
    