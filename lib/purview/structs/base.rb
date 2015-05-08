module Purview
  module Structs
    class Base < OpenStruct
      def [](key)
        key = key.to_sym unless key.is_a?(Symbol)
        raise NoMethodError unless respond_to?(key)
        send(key)
      end

      def []=(key, value)
        send("#{key}=", value)
      end

      def method_missing(method, *args, &block)
        raise NoMethodError if args.empty?
        super
      end
    end
  end
end
