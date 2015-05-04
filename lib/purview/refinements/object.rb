class Object
  def quoted
    "'#{self}'"
  end

  def sanitized
    self.to_s.sanitized
  end
end
