package com.nikhil.upstoxbarchart;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nikhil.upstoxbarchart.beans.BarOHLC;
import com.nikhil.upstoxbarchart.beans.BarOutputOHLC;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootApplication
public class UpstoxBarchartApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(UpstoxBarchartApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {



		ExecutorService executorService = Executors.newFixedThreadPool(100,
				(Runnable r) -> {
					Thread t = new Thread(r);
					t.setDaemon(true);
					return t;
				});




		LineIterator it = FileUtils.lineIterator(new File("trades.json"), "UTF-8");
		long s = System.currentTimeMillis();
		
		//create  a grouping of stockname-> List of all trades for particula stock
		Map<String, TreeSet<BarOHLC>> nameObjectMap =  new HashMap<>();
		

		while (it.hasNext()) {
			SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss:SSSSSSS");
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setDateFormat(df);
			String line = it.nextLine();
			BarOHLC barOHLC  = objectMapper.readValue(line,BarOHLC.class);
			nameObjectMap.putIfAbsent(barOHLC.getSym(),new TreeSet<>());
			nameObjectMap.computeIfPresent(barOHLC.getSym(),(k,v)->{
				v.add(barOHLC);
				return v;
			});
		}

			LineIterator.closeQuietly(it);
		//start--------------------------------
        // below code is just to test one stock and that too take first 100 trades only . 
        // Running for all stocks or even one with all thousands of trade take too much time to get result

		Map<String,List<BarOutputOHLC>> outputMap = new HashMap<>();
        Collection<BarOHLC> t = nameObjectMap.get("XXBTZUSD");

       TreeSet<BarOHLC> temp = new TreeSet<>(t);

       int i =0;
        Iterator<BarOHLC> iterator = temp.iterator();
        while (iterator.hasNext()){
            iterator.next();
            if (i++>100)
                iterator.remove();
        }
        nameObjectMap.clear();
       nameObjectMap.put("XXBTZUSD",temp);

//end -------------------------------------

        //here take each stock in map, extract list of trades for this stock and generate required output List 
        nameObjectMap.keySet().stream()
                .map(s1->CompletableFuture.supplyAsync(() -> processBarData(nameObjectMap.get(s1)),executorService))
                .map(future-> future.thenApply(list->outputMap.putIfAbsent(list.get(0).getSymbol(),list) ))
                .collect(Collectors.toList())
                .forEach(CompletableFuture::join);

		outputMap.get("XXBTZUSD").forEach(System.out::println);
		
	}

    /**
     * This method takes set of trades for a particular stock and generate output List as mentioned in problem
     * This is the main method of interest
     * @param barOHLCS
     * @return
     */
	private  List<BarOutputOHLC> processBarData(TreeSet<BarOHLC> barOHLCS) {

		//Date barTime = barOHLCS.first().getTS2();
		Instant barStartPlus15Seconds = barOHLCS.first().getTS2().plusSeconds(15);
		int bar_num = 1;

		List<BarOutputOHLC> ohlcs = new ArrayList<>();
		BarOHLC lastTickInBar = null;

		double v = 0.0;
		double h = 0.0;
		double l = Integer.MAX_VALUE;

		for (BarOHLC barOHLC : barOHLCS) {
		    h =  Math.max(h,barOHLC.getP());
		    l = Math.min(l,barOHLC.getP());
		    v+=barOHLC.getQ();
			Instant instant = barOHLC.getTS2();
            BarOutputOHLC lastBarOutputOHLC = ohlcs.size()==0?null:ohlcs.get(ohlcs.size() - 1);

			if (instant.isBefore(barStartPlus15Seconds) || instant.equals(barStartPlus15Seconds)){
			   ohlcs.add( BarOutputOHLC.builder()
                        .bar_num(bar_num)
                        .symbol(barOHLC.getSym())
                        .volume(v)
                        .o(barOHLC.getP())
                        .h(h)
                        .l(l)
                        .c(0.0)
                        .build());
			   lastTickInBar=barOHLC;
			   continue;
			}

			while (instant.isAfter(barStartPlus15Seconds.plusSeconds(15))){
				barStartPlus15Seconds = barStartPlus15Seconds.plusSeconds(15);
			}
			if (lastBarOutputOHLC!=null && lastTickInBar!=null)
			lastBarOutputOHLC.setC(lastTickInBar.getP());

			ohlcs.add(BarOutputOHLC.builder()
                    .bar_num(++bar_num)
                    .symbol(barOHLC.getSym())
                    .volume(v)
                    .o(barOHLC.getP())
                    .h(barOHLC.getP())
                    .l(barOHLC.getP())
                    .c(0.0)
                    .build()
            );

			lastTickInBar = barOHLC;


        }

    return ohlcs;
	}
}
