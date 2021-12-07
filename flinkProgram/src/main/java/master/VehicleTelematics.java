package main.java.master;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;


import java.util.Iterator;







public class VehicleTelematics{
	
	
	
	
	
	
	
	
	private static class AverageSpeedAggregate implements AggregateFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> createAccumulator() {
			return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(-1,0,0,0,0,0,0,0);
		}

		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> add(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> value, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> accumulator) {
			if (accumulator.f0 == -2 || ((value.f4 == 0 && ((value.f5 == 0 && value.f6 != 52) || (value.f5 == 1 && value.f6 != 56) )) || (value.f4 == 4 && ((value.f5 == 0 && value.f6 != 56) || (value.f5 == 1 && value.f6 != 52))))){
				return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(-2,0,0,0,0,0,0,0);
			}else if (accumulator.f0 == -1) {
				return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(value.f0,value.f0,value.f1,value.f3,value.f5,value.f7,value.f7,value.f6);
			}else{
	
				if (value.f6 == 52 && accumulator.f7 == 52 && value.f7 < accumulator.f5) {
					if (accumulator.f4 == 0) {
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(value.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,value.f7,accumulator.f6,value.f6);
					}else {
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,value.f0,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,value.f7,value.f6);	
					}
				}else if (value.f6 == 56 && accumulator.f7 == 56 && value.f7 > accumulator.f6){
					if (accumulator.f4 == 0) {
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,value.f0,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,value.f7,value.f6);
					}else {
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(value.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,value.f7,accumulator.f6,value.f6);	
					}
				}else{
					return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,value.f0,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,value.f7,value.f6);
				}
			}
		}

		@Override
		public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> getResult(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> accumulator) {
			if (accumulator.f4 == 0) {
				return new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,(int)(2.23694*(accumulator.f6-accumulator.f5)/(accumulator.f1-accumulator.f0))); //Take absolute vaues
			}else {
				return new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,(int)(2.23694*(accumulator.f5-accumulator.f6)/(accumulator.f1-accumulator.f0))); //Take absolute vaues
			}
		}

		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> merge(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> a, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> b) {
			return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(a.f0,b.f1,a.f2,a.f3,a.f4,a.f5,b.f6,b.f6);
		}
	}

	
	
	
	
	
	
	
	
	private static class AccidentAggregate implements AggregateFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> {
		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> createAccumulator() {
			return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE,0);
		}

		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> add(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> value, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> accumulator) {
			if (accumulator.f7 == 0) {
				System.out.println("First Accumulator: "+accumulator);
				return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> (value.f0,accumulator.f1,value.f1,value.f3,value.f6,value.f5,value.f7,accumulator.f7+1);
			}else {
				if (accumulator.f6 == value.f7) {
					if (value.f0 < accumulator.f1) {
						System.out.println("Unordered Accumulator: "+accumulator);
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> (accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,accumulator.f6,accumulator.f7+1);
					}else {
						System.out.println("Ordered Accumulator: "+accumulator);
						return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> (accumulator.f0,value.f0,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,accumulator.f6,accumulator.f7+1);
					}
				}else {
					return accumulator;
				}
			}
		}

		@Override
		public Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> getResult(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> accumulator) {
			if (accumulator.f7 == 4) {
				return new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,accumulator.f6);
			}else {
				return new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> (accumulator.f0,-1,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,accumulator.f6);
			}
		}

		@Override
		public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> merge(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> a, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> b) {
			System.out.println("\nMerge\n: "+a+b);
			return new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(a.f0,b.f1,a.f2,a.f3,a.f4,a.f5,b.f6,b.f6);
		}
	}
	
    private static class AccidentWindowFunction implements WindowFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, GlobalWindow> {
        public void apply(Tuple tuple, GlobalWindow countWindow, Iterable<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input, Collector<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> out) throws Exception {
            Iterator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> iterator = input.iterator();
            Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> first = iterator.next();
            Integer Time1 = -1;
            Integer Time2 = -1;
            Integer VID = -1;
            Integer Xway = -1;
            Integer Seg = -1;
            Integer Dir = -1;
            Integer Pos = -1;
            Integer Count = 0;
            if(first!=null){
                VID = first.f1;
                Time1 = first.f0;
                Xway = first.f3;
                Seg = first.f6;
                Dir = first.f5;
                Pos = first.f7;
                Count = 1;
            }
            while(iterator.hasNext()){
            	Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> next = iterator.next();
            	if (next.f7.equals(Pos)) {
            		Count++;
            	}
            	if (next.f0 < Time1) {
            		Time1 = next.f0;
            	}
            	if (next.f0 > Time2) {
            		Time2 = next.f0;
            	}
            }
            if (Count == 4) {
            	out.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(Time1,Time2,VID,Xway,Seg,Dir,Pos));
            }else {
            	out.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(Time1,-1,VID,Xway,Seg,Dir,Pos));
            }
        }
    }
	
	
	
	
	
	
	
	
	
    public static void main(String[] args){

        final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

        DataStream<String> s = env.readTextFile(params.get("input"));
        String outFilePath = args[3];
        
        SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s2 = s.flatMap(new FlatMapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
        	@Override
        	public void flatMap(String input, Collector<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> collector) {
        		String[] car = input.split(",");
        		collector.collect(new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(Integer.parseInt(car[0]),Integer.parseInt(car[1]),Integer.parseInt(car[2]),Integer.parseInt(car[3]),Integer.parseInt(car[4]),Integer.parseInt(car[5]),Integer.parseInt(car[6]),Integer.parseInt(car[7])));
        	}
        });
        
        
        
        
        
        
        
        
        // SPEED RADAR FUNCTIONALITY
        SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s3 = s2.filter(new FilterFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
        	@Override
        	public boolean filter(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> s_aux) {
        			return s_aux.f2>90;
        	}
        });
        
        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> s4 = s3.map(new MapFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>(){
        	@Override
        	public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> s3){
        		return new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>(s3.f0,s3.f1,s3.f3,s3.f6,s3.f5,s3.f2);
        	}
        });
        
        
        // Emit results
        s4.writeAsCsv(outFilePath+"/output/speedfines.csv",FileSystem.WriteMode.OVERWRITE);
        
        
        
        
        
        
        
        // AVERAGE SPEED FUNCTIONALITY
        SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s2_filtered = s2.filter(new FilterFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>(){
        	@Override
        	public boolean filter(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> s_aux) {
        		return s_aux.f6 >= 52 && s_aux.f6 <= 56;
        	}
        });
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream s_keyed = s2_filtered.assignTimestampsAndWatermarks(
        								new AscendingTimestampExtractor<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
        		                            @Override
        		                            public long extractAscendingTimestamp(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> element) {
        		                                return element.f0*1000; //*1000 to set seconds. Flink takes ms as default so if we have 30s we need 30000ms
        		                            }
        		                        }		
        						)
        						.keyBy(1);
        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> s4avg = s_keyed.window(EventTimeSessionWindows.withGap(Time.seconds(35))) //These 30 seconds refer to field 0 value //Add lateness
        																									.aggregate(new AverageSpeedAggregate());
        																												   
        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> s4avg_speedup = s4avg.filter(new FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
        	@Override
        	public boolean filter(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> s_aux) {
        			return s_aux.f5>60;
        	}
        });
        
        
        // Emit results
        s4avg_speedup.writeAsCsv(outFilePath+"/output/avgspeedfines.csv",FileSystem.WriteMode.OVERWRITE);
        
        
        
        
        
        
        
        
        //Accident Reporter Functionality
        KeyedStream s_keyed_accident = s2.assignTimestampsAndWatermarks(
				new AscendingTimestampExtractor<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> element) {
                        return element.f0*1000; //*1000 to set seconds. Flink takes ms as default so if we have 30s we need 30000ms
                    }
                }		
		)
		.keyBy(1);
        
        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s_accidents = s_keyed_accident.countWindow(4,1)
																																  .apply(new AccidentWindowFunction());
        
        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s_accidented = s_accidents.filter(new FilterFunction<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
        	@Override
        	public boolean filter(Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> s_aux) {
        			return s_aux.f1>0;
        	}
        });
        
        // Emit results
        s_accidented.writeAsCsv(outFilePath+"/output/accidents.csv",FileSystem.WriteMode.OVERWRITE);
        						

        				
        						
        

        	
        
        


        
        
        
        
        
        
        
        
        
        
        
        
        
        try{
        	env.execute();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

