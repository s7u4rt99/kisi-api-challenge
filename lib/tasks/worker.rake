# frozen_string_literal: true

namespace(:worker) do
  desc("Run the worker")
  task(run: :environment) do
    # See https://googleapis.dev/ruby/google-cloud-pubsub/latest/index.html

    puts("Worker starting...")

    topic = Pubsub.new.topic("projects/code-challenge/topics/challenge")
    subscription = topic.subscribe("whatever")
    if subscription.nil?
      puts("Subscription not found: 'whatever'")
    else
      subscriber = subscription.listen do |message|
        job = ActiveJob::Base.deserialize(JSON.parse(message.data))
        puts("processing job...")
        job.perform_now
        message.acknowledge!
      end

      subscriber.on_error do |error|
        puts(error)
      end

      subscriber.start
    end

    # Block, letting processing threads continue in the background
    sleep
  end
end
