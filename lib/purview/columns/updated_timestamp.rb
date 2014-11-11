module Purview
  module Columns
    class UpdatedTimestamp < Timestamp
      private

      def default_opts
        super.merge(:allow_blank => false)
      end
    end
  end
end
