class Time
  def quoted
    "'#{self.strftime('%F %T')}'"
  end
end
