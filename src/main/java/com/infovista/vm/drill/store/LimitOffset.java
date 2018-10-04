package com.infovista.vm.drill.store;

public class LimitOffset {
	
		private int limit;
		private int offset = 0;
		
		public LimitOffset(int limit, int offset)
		{
			this.limit = limit;
			this.offset = offset;
		}

		public int getLimit() {
			return limit;
		}

		public int getOffset() {
			return offset;
		}
		
		@Override
		public String toString() {
			return "Limit: " + limit + ", Offset: " + offset;
		}

}
