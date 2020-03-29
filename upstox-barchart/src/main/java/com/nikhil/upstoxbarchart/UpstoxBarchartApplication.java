package com.nikhil.upstoxbarchart;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nikhil.upstoxbarchart.beans.BarOHLC;
import com.nikhil.upstoxbarchart.beans.BarOutputOHLC;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class UpstoxBarchartApplication implements CommandLineRunner {

	public static void main(String[] args)  {
		SpringApplication.run(UpstoxBarchartApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		LineIterator it = FileUtils.lineIterator(new File("trades.json"), "UTF-8");

		//create  a grouping of stockname-> List of all trades for particula stock
		Map<String, TreeSet<BarOHLC>> nameObjectMap =  new HashMap<>();

		Set<String> inputStocks = new HashSet<>(Arrays.asList(args));


		while (it.hasNext()) {
			SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss:SSSSSSS");
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setDateFormat(df);
			String line = it.nextLine();
			BarOHLC barOHLC  = objectMapper.readValue(line,BarOHLC.class);
			if (inputStocks.contains(barOHLC.getSym())) {
				nameObjectMap.putIfAbsent(barOHLC.getSym(), new TreeSet<>());
				nameObjectMap.computeIfPresent(barOHLC.getSym(), (k, v) -> {
					v.add(barOHLC);
					return v;
				});
			}
		}

		ExecutorService executorService = Executors.newFixedThreadPool(Math.min(Runtime.getRuntime().availableProcessors(),nameObjectMap.size()),
				(Runnable r) -> {
					Thread t = new Thread(r);
					t.setDaemon(true);
					return t;
				});

			LineIterator.closeQuietly(it);

		Map<String,List<BarOutputOHLC>> outputMap = new HashMap<>();

		//start--------------------------------
        // below code is just to test one stock and that too take first 100 trades only .
        // Running for all stocks or even one with all thousands of trade take too much time to get result

		for (String stockName : args) {
			Collection<BarOHLC> t = nameObjectMap.get(stockName);
			TreeSet<BarOHLC> temp = new TreeSet<>(t);
			int i = 0;
			Iterator<BarOHLC> iterator = temp.iterator();
			/*while (iterator.hasNext()) {
				iterator.next();
				if (++i > 100)
					iterator.remove();
			}*/
			nameObjectMap.put(stockName, temp);
			t.clear();
		}
//end -------------------------------------

        //here take each stock in map, extract list of trades for this stock and generate required output List
        nameObjectMap.keySet().stream()
                .map(s1->CompletableFuture.supplyAsync(() -> new TaskRunner().processBarData(nameObjectMap.get(s1)),executorService))
                .map(future-> future.thenApply(list->outputMap.putIfAbsent(list.get(0).getSymbol(),list) ))
                .collect(Collectors.toList())
                .forEach(CompletableFuture::join);

		outputMap.values().forEach(lst-> lst.forEach(System.out::println));

	}
}


class TaskRunner {
	/**
	 * This method takes set of trades for a particular stock and generate output List as mentioned in problem
	 * This is the main method of interest
	 * @param barOHLCS
	 * @return
	 */
	public  List<BarOutputOHLC> processBarData(TreeSet<BarOHLC> barOHLCS) {

		Instant barStartPlus15Seconds = barOHLCS.first().getTS2().plusSeconds(15);
		int bar_num = 1;

		List<BarOutputOHLC> ohlcs = new ArrayList<>();
		BarOHLC lastTickInBar = null;

		double v = 0.0;
		double h = 0.0;
		double l = Integer.MAX_VALUE;
		BarOutputOHLC lastBarOutputOHLC =null;
		int size=0;
		for (BarOHLC barOHLC : barOHLCS) {
			++size;
			h =  Math.max(h,barOHLC.getP());
			l = Math.min(l,barOHLC.getP());
			v+=barOHLC.getQ();
			Instant instant = barOHLC.getTS2();
			lastBarOutputOHLC = ohlcs.size()==0?null:ohlcs.get(ohlcs.size() - 1);

			if (instant.isBefore(barStartPlus15Seconds) || instant.equals(barStartPlus15Seconds)){
				ohlcs.add( BarOutputOHLC.builder()
						.bar_num(bar_num)
						.symbol(barOHLC.getSym())
						.event("ohlc_notify")
						.volume(v)
						.o(h)
						.h(h)
						.l(l)
						//edge case if this is last trade in 15 second window.Set close
						.c(size==barOHLCS.size()?barOHLC.getP():0.0)
						.build());
				lastTickInBar=barOHLC;
				continue;
			}

			while (instant.isAfter(barStartPlus15Seconds.plusSeconds(15))){
				barStartPlus15Seconds = barStartPlus15Seconds.plusSeconds(15);
				bar_num++;
			}
			if (instant.isAfter(barStartPlus15Seconds))
				barStartPlus15Seconds = barStartPlus15Seconds.plusSeconds(15);
			if (lastBarOutputOHLC!=null && lastTickInBar!=null)
				lastBarOutputOHLC.setC(lastTickInBar.getP());

			ohlcs.add(BarOutputOHLC.builder()
					.bar_num(++bar_num)
					.symbol(barOHLC.getSym())
					.event("ohlc_notify")
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
