module Purview
  module Types
    class Boolean
      def self.parse(value)
        !!(value =~ /\A(true|t|yes|y|1)\z/i)
      end
    end

    class Date
      def self.parse(value)
        ::Date.parse(value)
      end
    end

    class Float
      def self.parse(value)
        Float(value)
      end
    end

    class Integer
      def self.parse(value)
        Integer(value)
      end
    end

    class Money
      def self.parse(value)
        Float(value)
      end
    end

    class String
      def self.parse(value)
        String(value)
      end
    end

    class Text
      def self.parse(value)
        String(value)
      end
    end

    class Time
      def self.parse(value)
        ::Time.parse(value)
      end
    end

    class Timestamp
      def self.parse(value)
        ::Time.parse(value)
      end
    end

    class UUID
      def self.parse(value)
        String(value)
      end
    end
  end
end
