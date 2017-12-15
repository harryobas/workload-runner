require 'net/ssh'
require 'optparse'

module Experiment
  module Config
    THROTT_PORT = 20000
    RUNS = 10
    USR_ID = 'ioi600'
    HiBench_home = '/mnt/HiBench'
    Files = {
      :hibench_report => "#{HiBench_home}/report/hibench.report",
      :host_list => "#{ENV['HOME']}/host_list",
      :set_file => "#{ENV['HOME']}/set_file",
      :thrott_client => "#{ENV['HOME']}/bandwidth-throttler/shape_traffic_client.py",
      :wordcount_prepare => "#{HiBench_home}/bin/workloads/micro/wordcount/prepare/",
      :wordcount_run => "#{HiBench_home}/bin/workloads/micro/wordcount/spark/",
      :sort_prepare => "#{HiBench_home}/bin/workloads/micro/sort/prepare/",
      :sort_run => "#{HiBench_home}/bin/workloads/micro/sort/spark/",
      :terasort_prepare => "#{HiBench_home}/bin/workloads/micro/terasort/prepare/",
      :terasort_run => "#{HiBench_home}/bin/workloads/micro/terasort/spark/",
      :bayes_prepare => "#{HiBench_home}/bin/workloads/ml/bayes/prepare/",
      :bayes_run => "#{HiBench_home}/bin/workloads/ml/bayes/spark/",
      :kmeans_prepare => "#{HiBench_home}/bin/workloads/ml/kmeans/prepare/",
      :kmeans_run => "#{HiBench_home}/bin/workloads/ml/kmeans/spark/",
      :pagerank_prepare => "#{HiBench_home}/bin/workloads/websearch/pagerank/prepare/",
      :pagerank_run => "#{HiBench_home}/bin/workloads/websearch/pagerank/spark/",
      :wordcount_metrics => "#{ENV['HOME']}/results/micro/wordcount.metrics",
      :sort_metrics => "#{ENV['HOME']}/results/micro/sort.metrics",
      :terasort_metrics => "#{ENV['HOME']}/results/micro/terasort.metrics",
      :bayes_metrics => "#{ENV['HOME']}/results/ml/bayes.metrics",
      :kmeans_metrics => "#{ENV['HOME']}/results/ml/kmeans.metrics",
      :pagerank_metrics => "#{ENV['HOME']}/results/websearch/pagerank.metrics"
    }

    Workload = {
      :micro => {:wordcount => [Files[:wordcount_prepare], Files[:wordcount_run]], :sort => [Files[:sort_prepare], Files[:sort_run]], :terasort => [Files[:terasort_prepare], Files[:terasort_run]]},
      :ml  => {:bayes => [Files[:bayes_prepare], Files[:bayes_run]], :kmeans => [Files[:kmeans_prepare], Files[:kmeans_run]]},
      :web_search => {:pagerank => [Files[:pagerank_prepare], Files[:pagerank_run]]}
    }

    Cloudcase = {
      :A => %w[440mbit 497mbit 489mbit 617mbit 290mbit 286mbit 359mbit 356mbit 244mbit 255mbit 218mbit 226mbit 63mbit 135mbit 116mbit 115mbit],
      :B => %w[916mbit 957mbit 924mbit 801mbit 754mbit 676mbit 758mbit 654mbit 584mbit 635mbit 557mbit 623mbit 443mbit 501mbit 409mbit 336mbit],
      :C => %w[938mbit 940mbit 939mbit 940mbit 921mbit 910mbit 930mbit 904mbit 855mbit 846mbit 894mbit 870mbit 680mbit 692mbit 729mbit 792mbit],
      :D => %w[243mbit 282mbit 281mbit 292mbit 189mbit 169mbit 184mbit 190mbit 166mbit 168mbit 167mbit 138mbit 117mbit 130mbit 137mbit 117mbit],
      :E => %w[858mbit 858mbit 856mbit 856mbit 852mbit 853mbit 852mbit 853mbit 834mbit 848mbit 844mbit 830mbit 797mbit 791mbit 809mbit 822mbit],
      :F => %w[900mbit 846mbit 887mbit 853mbit 791mbit 800mbit 789mbit 814mbit 741mbit 736mbit 770mbit 745mbit 690mbit 490mbit 703mbit 301mbit],
      :G => %w[602mbit 604mbit 614mbit 613mbit 576mbit 584mbit 556mbit 593mbit 535mbit 538mbit 533mbit 534mbit 417mbit 480mbit 400mbit 487mbit],
      :H => %w[953mbit 927mbit 679mbit 795mbit 552mbit 624mbit 531mbit 526mbit 436mbit 474mbit 466mbit 461mbit 332mbit 251mbit 355mbit 260mbit]
      }

  end

  class WorkloadRunner
    include Config
    attr_reader :thrott_set
    
    def initialize(cloudcase=nil)
      case cloudcase
      when 'a'
        @cloudcase = Cloudcase[:A]
      when 'b'
        @cloudcase = Cloudcase[:B]
      when 'c'
        @cloudcase = Cloudcase[:C]
      when 'd'
        @cloudcase = Cloudcase[:D]
      when 'e'
        @cloudcase = Cloudcase[:E]
      when 'f'
        @cloudcase = Cloudcase[:F]
      when 'g'
        @cloudcase = Cloudcase[:G]
      when 'h'
        @cloudcase = Cloudcase[:H]
      end
    end

    def self.get_cluster_hosts
      File.open(Files[:host_list]) do |f|
        f.readlines.map { |l| l.split(' ').first  }
      end
    end

    def self.get_master_host
      File.open(Files[:host_list]) do |f|
        lines = f.readlines
        lines.first.split(' ').first
      end
    end

    def throttle_cluster_bandwidth(bw_conf)
      hosts = self.class.get_cluster_hosts
      bw_conf.each do |bw|
        hosts.each do |h|
          hosts2 = hosts.dup
          send_throttle_request(h, bw, hosts2 - [h])
        end
      end
      @thrott_set = true
    end


    def send_throttle_request(host, bw, vms)
      vms.each do |vm|
        system("python #{Files[:thrott_client]} --port #{THROTT_PORT} --set #{host}:#{vm}:#{bw}")
      end
    end


    def remove_bandwidth_throttle
      self.class.get_cluster_hosts.each do |vm|
        system("python #{Files[:thrott_client]} --port #{THROTT_PORT} --reset #{vm}")
      end
    end

    def prepare_and_run_workload(work_info)
      threads = []
      cmd = <<-eos
      cd #{work_info.first}
      ./prepare.sh
      eos
      cmd_2 = <<-eos
      cd #{work_info.last}
      ./run.sh
      eos
      ssh = Net::SSH.start("#{self.class.get_master_host}", USR_ID)
      res = ssh.exec!(cmd)
      puts res
      throttle_cluster_bandwidth(@cloudcase) unless @cloudcase == nil
      RUNS.times{puts ssh.exec!(cmd_2)}

      collect_metrics(ssh, work_info)
   end

    def collect_metrics(ssh, work_info)
      cmd = "cat #{Files[:hibench_report]}"
    cmd_2 = "truncate -s 0 #{Files[:hibench_report]}"
    case work_info
    when Workload[:micro][:wordcount]
      File.truncate(Files[:wordcount_metrics], 0) unless File.zero?(Files[:wordcount_metrics])
      File.open(Files[:wordcount_metrics], 'w') do |f|
        f.puts "#{ssh.exec!(cmd)}"
      end
      ssh.exec!(cmd_2)
      ssh.close
    when Workload[:micro][:sort]
      File.truncate(Files[:sort_metrics], 0) unless File.zero?(Files[:sort_metrics])
      File.open(Files[:sort_metrics], 'w') do |f|
        f.puts "#{ssh.exec!(cmd)}"
      end
      ssh.exec!(cmd_2)
      ssh.close
    when Workload[:micro][:terasort]
      File.truncate(Files[:terasort_metrics], 0) unless File.zero?(Files[:terasort_metrics])
      File.open(Files[:terasort_metrics], 'w') do |f|
        f.puts "#{ssh.exec!(cmd)}"
      end
      ssh.exec!(cmd_2)
      ssh.close
    when Workload[:ml][:bayes]
      File.truncate(Files[:bayes_metrics], 0) unless File.zero?(Files[:bayes_metrics])
      File.open(Files[:bayes_metrics], 'w') do |f|
        f.puts "#{ssh.exec!(cmd)}"
      end
      ssh.exec!(cmd_2)
      ssh.close
    when Workload[:ml][:kmeans]
      File.truncate(Files[:kmeans_metrics], 0) unless File.zero?(Files[:kmeans_metrics])
      File.open(Files[:kmeans_metrics], 'w') do |f|
        f.puts "#{ssh.exec!(cmd)}"
      end
      ssh.exec!(cmd_2)
      ssh.close
    when Workload[:web_search][:pagerank]
      File.truncate(Files[:pagerank_metrics], 0) unless File.zero?(Files[:pagerank_metrics])
    File.open(Files[:pagerank_metrics], 'w') do |f|
      f.puts "#{ssh.exec!(cmd)}"
    end
    ssh.exec!(cmd_2)
    ssh.close
    else
      puts "unknown workload"
    end
  end

    private :collect_metrics, :send_throttle_request
  end

end

options = {}
option_parser = OptionParser.new do |opt|

  opt.on("-w", "--wordcount") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:micro][:wordcount]
  end
  opt.on("s", "--sort") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:micro][:sort]
  end
  opt.on("-t", "--tera") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:micro][:terasort]
  end
  opt.on("-b", "--bayes") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:ml][:bayes]
  end
  opt.on("-k", "--kmeans") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:ml][:kmeans]
  end
  opt.on("-p", "--pagerank") do
    options[:workload] = Experiment::WorkloadRunner::Workload[:web_search][:pagerank]
  end
end

option_parser.parse!

if ARGV.any?
  runner = Experiment::WorkloadRunner.new(ARGV[0])
else
  runner = Experiment::WorkloadRunner.new()
end
runner.prepare_and_run_workload(options[:workload])
runner.remove_bandwidth_throttle if runner.thrott_set

exit(0)
