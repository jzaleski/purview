module Purview
  module Structs
    class Base < OpenStruct
      def method_missing(method_name, *args, &block)
        raise NoMethodError if args.size.zero?
        super
      end
    end
  end
end
