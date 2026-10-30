using RRC.Abstractions;

namespace RRC.Student;

public sealed class TcpRequestPolicy : IRequestPolicy
{
    private const double Alpha = 1.0 / 8.0;
    private const double Beta = 1.0 / 4.0;
    private const double K = 4.0;

    private const int WarmupSamples = 30;
    private const double WarmupExtraK = 4.0;

    private const double InitialRtoMs = 1000;
    private const double MinRtoMs = 5;
    private const double MaxRtoMs = 60_000;

    private const double BackoffUp = 2.0;
    private const double BackoffDown = 0.999;
    private const double MaxBackoff = 16;
    private const double BackoffGrowthRate = 0.10;

    private const int MaxAttempts = 4;
    private const double CongestionThreshold = 0.30;
    private const double LossGain = 0.05;
    private const double RetryDelayFractionOfRto = 0.25;

    private readonly object _sync = new();

    private bool _hasSample;
    private int _samples;
    private double _srttMs;
    private double _rttvarMs;
    private double _backoff = 1.0;
    private double _timeoutRate;

    public TimeSpan GetTimeout(RequestContext context, Random random)
    {
        lock (_sync)
        {
            double rto = CurrentRtoMs();

            int doublings = Math.Max(0, context.Attempt - 1);
            rto *= Math.Pow(2, Math.Min(doublings, 6));

            return TimeSpan.FromMilliseconds(Math.Min(rto, MaxRtoMs));
        }
    }

    public RetryDecision OnResult(RequestObservation observation, Random random)
    {
        lock (_sync)
        {
            bool timedOut = observation.Outcome == RequestOutcome.Timeout;
            double elapsedMs = observation.Elapsed.TotalMilliseconds;

            _timeoutRate += LossGain * ((timedOut ? 1.0 : 0.0) - _timeoutRate);

            if (!timedOut)
            {
                _backoff = Math.Max(1.0, _backoff * BackoffDown);

                if (observation.Attempt == 1)
                    UpdateRtt(elapsedMs);

                return RetryDecision.NoRetry();
            }

            if (_timeoutRate >= BackoffGrowthRate)
                _backoff = Math.Min(MaxBackoff, _backoff * BackoffUp);

            if (observation.Attempt == 1)
                UpdateRtt(elapsedMs);

            if (_timeoutRate >= CongestionThreshold)
                return RetryDecision.NoRetry();

            if (observation.Attempt >= MaxAttempts)
                return RetryDecision.NoRetry();

            double delay = CurrentRtoMs() * RetryDelayFractionOfRto
                           * Math.Pow(2, observation.Attempt - 1);

            delay = Math.Min(delay, MaxRtoMs);
            delay *= 0.5 + 0.5 * random.NextDouble();

            return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(delay));
        }
    }

    private double CurrentRtoMs()
    {
        double rto;

        if (!_hasSample)
        {
            rto = InitialRtoMs;
        }
        else
        {
            double warmup = Math.Max(0.0, 1.0 - (double)_samples / WarmupSamples);
            double k = K + WarmupExtraK * warmup;
            rto = _srttMs + k * _rttvarMs;
        }

        rto *= _backoff;
        return Math.Clamp(rto, MinRtoMs, MaxRtoMs);
    }

    private void UpdateRtt(double sampleMs)
    {
        _samples++;

        if (!_hasSample)
        {
            _srttMs = sampleMs;
            _rttvarMs = sampleMs / 2.0;
            _hasSample = true;
            return;
        }

        _rttvarMs = (1 - Beta) * _rttvarMs + Beta * Math.Abs(_srttMs - sampleMs);
        _srttMs = (1 - Alpha) * _srttMs + Alpha * sampleMs;
    }
}