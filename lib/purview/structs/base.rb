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
        method = method.to_sym unless method.is_a?(Symbol)
        raise NoMethodError if args.empty? && !respond_to?(method)
        super
      end
    end
  end
end
