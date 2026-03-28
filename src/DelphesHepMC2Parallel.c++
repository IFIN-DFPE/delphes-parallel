#include <chrono>
#include <iostream>

#include <HepMC/IO_GenEvent.h>
#include <HepMC/GenEvent.h>

int main()
{
    HepMC::IO_GenEvent input("/data/gmajeri/delphes-parallel/tests/events.hepmc", std::ios::in);

    HepMC::IO_GenEvent output("/data/gmajeri/delphes-parallel/tests/copied.hepmc", std::ios::out);

    int events_count = 0;

    using Clock = std::chrono::steady_clock;
    using Instant = std::chrono::time_point<Clock>;
    using Second = std::chrono::duration<double, std::ratio<1>>;

    Instant start_time{Clock::now()};

    Instant batch_start_time{Clock::now()};

    HepMC::GenEvent *event = input.read_next_event();
    while (event)
    {
        if (events_count % 100 == 1)
        {
            Instant batch_end_time{Clock::now()};
            double elapsed_time = std::chrono::duration_cast<Second>(batch_end_time - batch_start_time).count();
            double average_time = elapsed_time / 100;

            std::cout << "Processing event number #" << events_count
                      << ", its internal number: " << event->event_number()
                      << std::endl;

            std::cout << "Average time per event: " << average_time << " seconds/event" << std::endl;

            batch_start_time = Clock::now();
        }

        output.write_event(event);
        ++events_count;
        delete event;

        input >> event;
    }

    Instant end_time{Clock::now()};
    double elapsed_time = std::chrono::duration_cast<Second>(end_time - start_time).count();

    std::cout << "Processed " << events_count << "." << std::endl;
    std::cout << "Took " << elapsed_time << " seconds." << std::endl;
    std::cout << "Finished!" << std::endl;

    return 0;
}
