module Purview
  module Tables
    class Raw < BaseSyncable
      def name
        "#{super}_raw"
      end

      def window_size
        opts[:window_size] || (24 * 60 * 60)
      end
    end
  end
end
