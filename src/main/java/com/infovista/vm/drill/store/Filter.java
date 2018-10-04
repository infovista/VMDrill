package com.infovista.vm.drill.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Filter {
	Map<String, List<FilterDesc>> filters;
	
	public Filter() {
		filters = new HashMap<>();
	}
	
	static public class FilterDesc{
		String compareOp;
		Object value;
	}
	
	static public Filter mergeFilters(Filter current, Filter nextFilter) {
		if(current == null)
			return nextFilter;
		if(nextFilter == null)
			return current;
		
		Filter resultFilter = new Filter();
		Map<String, List<FilterDesc>> concatFilter  = Stream.concat(nextFilter.filters.entrySet().stream(),current.filters.entrySet().stream())
				.collect(Collectors.toMap(entry ->entry.getKey(), entry ->entry.getValue(),Filter::mergeLists));
		
		resultFilter.filters = concatFilter;
		return resultFilter;
	}
	
	static public List<FilterDesc> mergeLists(List<FilterDesc> list1, List<FilterDesc> list2){
		return Stream.concat(list1.stream(), list2.stream())
				   .collect(Collectors.toList());
	}

}
