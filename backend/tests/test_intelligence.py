import unittest

from backend.core.intelligence.deduplication import filter_semantic_duplicates
from backend.core.intelligence.scoring import compute_clip_score
from backend.core.intelligence.segmentation import semantic_segment


class IntelligenceTests(unittest.TestCase):
    def test_semantic_segment_outputs_scores(self):
        words = [
            {"type": "word", "word": "Imagine", "startTime": 0.0, "endTime": 0.2},
            {"type": "word", "word": "this", "startTime": 0.2, "endTime": 0.3},
            {"type": "word", "word": "works.", "startTime": 0.3, "endTime": 0.5},
            {"type": "word", "word": "However", "startTime": 1.0, "endTime": 1.2},
            {"type": "word", "word": "results", "startTime": 1.2, "endTime": 1.4},
            {"type": "word", "word": "vary!", "startTime": 1.4, "endTime": 1.7},
        ]
        segments = semantic_segment(words)
        self.assertGreaterEqual(len(segments), 1)
        self.assertIn("emotion_score", segments[0])
        self.assertIn("topic_score", segments[0])
        self.assertIn("hook_score", segments[0])

    def test_compute_score_range(self):
        score, breakdown = compute_clip_score(
            {"llm_score": 88, "curiosity_score": 60},
            {"start": 0.0, "end": 10.0, "text": "What if this shocking method works?", "emotion_score": 0.8, "hook_score": 0.7},
            None,
        )
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)
        self.assertIn("speech_speed_spike", breakdown)

    def test_deduplicates_similar_clips(self):
        clips = [
            {"start": 0.0, "end": 20.0, "score": 92, "title": "Top growth hack", "summary": "Use this growth strategy now"},
            {"start": 2.0, "end": 22.0, "score": 91, "title": "Top growth hack", "summary": "Use this growth strategy now"},
            {"start": 30.0, "end": 55.0, "score": 85, "title": "Different topic", "summary": "A new idea"},
        ]
        filtered = filter_semantic_duplicates(clips)
        self.assertEqual(len(filtered), 2)


if __name__ == "__main__":
    unittest.main()
