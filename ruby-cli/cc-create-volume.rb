# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'control_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = ControlCenter::Client.new(protocol)

  transport.open()

  req = CreateVolumeRequest.new()    
  req.name = ARGV[1]
  req.volumeSize = ARGV[2].to_i * (2**10)**2
  req.volumeType = ARGV[3].to_i
  req.accountId = ARGV[4].to_i

  pp req
  result = client.createVolume(req)
  pp result
  
  transport.close()
rescue
  puts $!
end
