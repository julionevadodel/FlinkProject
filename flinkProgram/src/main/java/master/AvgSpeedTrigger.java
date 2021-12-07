package main.java.master;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.api.java.tuple.Tuple13;


public class AvgSpeedTrigger extends Trigger<Tuple13<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, GlobalWindow> {
	
	public AvgSpeedTrigger() {
	}
	@Override
	public TriggerResult onElement(Tuple13<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception{
		if (element.f5 == 0) {
			if (element.f6 >= 52 && element.f6 <= 56) {
				return TriggerResult.CONTINUE;
			}else if (element.f6 == 57) {
				return TriggerResult.FIRE_AND_PURGE;
			}else {
				return TriggerResult.PURGE;
			}
		}else if (element.f5 == 1){
			if (element.f6 >= 52 && element.f6 <= 56) {
				return TriggerResult.CONTINUE;
			}else if (element.f6 == 51) {
				return TriggerResult.FIRE_AND_PURGE;
			}else {
				return TriggerResult.PURGE;
			}
		}else {
			return TriggerResult.PURGE;
		}
	}
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception{
    	return TriggerResult.CONTINUE;
    }
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception{
    	return TriggerResult.CONTINUE;
    }
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception{
    	
    };

}
